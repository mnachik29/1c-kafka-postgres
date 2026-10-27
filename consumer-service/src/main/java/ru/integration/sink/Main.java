package ru.integration.sink;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.flywaydb.core.Flyway;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.integration.sink.db.CatalogRepository;
import ru.integration.sink.kafka.DlqPublisher;
import ru.integration.sink.model.EventParser;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicReference;

import static net.logstash.logback.argument.StructuredArguments.kv;

/**
 * Consumer-service: читает события справочников 1С из Kafka и пишет их в PostgreSQL.
 * Offset коммитится только после успешной транзакции в БД (at-least-once + идемпотентный upsert).
 */
public final class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);
    private static final Duration STALE_POLL = Duration.ofSeconds(60);

    private static volatile boolean running = true;
    private static final AtomicReference<KafkaConsumer<String, String>> current = new AtomicReference<>();

    private Main() {
    }

    public static void main(String[] args) throws Exception {
        Config config = Config.fromEnv();
        ObjectMapper mapper = new ObjectMapper();
        Metrics metrics = new Metrics();

        HikariDataSource dataSource = dataSource(config);
        migrate(config, dataSource);
        CatalogRepository repository = new CatalogRepository(dataSource);
        startHealthServer(config, repository, metrics, mapper);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            running = false;
            KafkaConsumer<String, String> c = current.get();
            if (c != null) {
                c.wakeup();
            }
        }));

        try (DlqPublisher dlq = new DlqPublisher(config)) {
            BatchProcessor processor = new BatchProcessor(config, new EventParser(mapper), repository, dlq, metrics);
            long backoff = config.retryInitialBackoff().toMillis();
            while (running) {
                try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerProps(config))) {
                    current.set(consumer);
                    consumer.subscribe(config.topics());
                    log.info("Consumer started", kv("group_id", config.groupId()), kv("topics", config.topics()));
                    while (running) {
                        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                        metrics.lastPollAtMillis = System.currentTimeMillis();
                        if (records.isEmpty()) {
                            continue;
                        }
                        processor.process(records);
                        consumer.commitSync();
                        backoff = config.retryInitialBackoff().toMillis();
                    }
                } catch (WakeupException e) {
                    if (running) {
                        log.warn("Unexpected consumer wakeup");
                    }
                } catch (Exception e) {
                    // Offset не закоммичен: после пересоздания consumer пакет будет прочитан повторно
                    metrics.restarts.incrementAndGet();
                    log.error("Consumer loop failed, restarting from last committed offsets",
                            kv("backoff_ms", backoff), kv("error", e.toString()), e);
                    sleep(backoff);
                    backoff = Math.min(backoff * 2, 60_000);
                } finally {
                    current.set(null);
                }
            }
        } finally {
            dataSource.close();
            log.info("Consumer stopped");
        }
    }

    private static Properties consumerProps(Config config) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, config.kafkaBootstrap());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, config.groupId());
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, "consumer-service");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, String.valueOf(config.maxPollRecords()));
        props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        return props;
    }

    private static HikariDataSource dataSource(Config config) {
        HikariConfig hc = new HikariConfig();
        hc.setJdbcUrl(config.jdbcUrl());
        hc.setUsername(config.dbUser());
        hc.setPassword(config.dbPassword());
        hc.setMaximumPoolSize(3);
        hc.setPoolName("pg-sink");
        hc.setConnectionTimeout(5_000);
        hc.setInitializationFailTimeout(-1); // не падать на старте, если БД ещё поднимается
        return new HikariDataSource(hc);
    }

    private static void migrate(Config config, HikariDataSource dataSource) throws InterruptedException {
        Flyway flyway = Flyway.configure()
                .dataSource(dataSource)
                .locations(config.migrationsLocation())
                .load();
        long backoff = config.retryInitialBackoff().toMillis();
        for (int attempt = 1; ; attempt++) {
            try {
                var result = flyway.migrate();
                log.info("Database migrations applied", kv("executed", result.migrationsExecuted),
                        kv("schema_version", result.targetSchemaVersion));
                return;
            } catch (Exception e) {
                if (attempt >= config.retryMaxAttempts() * 2) {
                    log.error("Database migration failed", kv("error", e.toString()), e);
                    throw e;
                }
                log.warn("Database is not ready for migration, will retry", kv("attempt", attempt),
                        kv("error", e.getMessage()));
                Thread.sleep(backoff);
                backoff = Math.min(backoff * 2, 15_000);
            }
        }
    }

    private static void startHealthServer(Config config, CatalogRepository repository, Metrics metrics,
                                          ObjectMapper mapper) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress(config.healthPort()), 0);
        server.createContext("/health", exchange -> {
            boolean db = repository.isAvailable();
            boolean polling = System.currentTimeMillis() - metrics.lastPollAtMillis < STALE_POLL.toMillis();
            Map<String, Object> body = new LinkedHashMap<>();
            body.put("status", db && polling ? "UP" : "DOWN");
            body.put("postgres", db ? "UP" : "DOWN");
            body.put("kafka_consumer", polling ? "UP" : "STALLED");
            body.put("messages_written", metrics.written.get());
            body.put("db_errors", metrics.dbErrors.get());
            send(exchange, db && polling ? 200 : 503, "application/json", mapper.writeValueAsString(body));
        });
        server.createContext("/metrics", exchange ->
                send(exchange, 200, "text/plain; version=0.0.4", metrics.prometheus()));
        server.start();
    }

    private static void send(HttpExchange exchange, int status, String contentType, String body) throws IOException {
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type", contentType);
        exchange.sendResponseHeaders(status, bytes.length);
        try (OutputStream out = exchange.getResponseBody()) {
            out.write(bytes);
        }
    }

    private static void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            running = false;
        }
    }
}
