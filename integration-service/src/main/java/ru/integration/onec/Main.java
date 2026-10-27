package ru.integration.onec;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.integration.onec.kafka.EventPublisher;
import ru.integration.onec.odata.ODataClient;
import ru.integration.onec.sync.Catalog;
import ru.integration.onec.sync.StateStore;
import ru.integration.onec.sync.SyncService;

import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static net.logstash.logback.argument.StructuredArguments.kv;

/**
 * Точка входа.
 * <pre>
 *   serve                                   — healthcheck + (опционально) инкрементальная синхронизация по расписанию
 *   sync --mode full|incremental
 *        [--entity all|ownership_forms|counterparties]
 *        [--since 2026-07-10T12:00:00]      — явная метка для инкрементального режима (UTC)
 *        [--reset-state]                    — сбросить сохранённые метки перед запуском
 * </pre>
 */
public final class Main {

    private static final Logger log = LoggerFactory.getLogger(Main.class);

    private Main() {
    }

    public static void main(String[] args) {
        if (args.length == 0) {
            usage();
            System.exit(2);
        }
        try {
            Config config = Config.fromEnv();
            ObjectMapper mapper = new ObjectMapper()
                    .registerModule(new JavaTimeModule())
                    .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
            switch (args[0]) {
                case "sync" -> System.exit(sync(config, mapper, Arrays.copyOfRange(args, 1, args.length)));
                case "serve" -> serve(config, mapper);
                default -> {
                    usage();
                    System.exit(2);
                }
            }
        } catch (IllegalArgumentException e) {
            log.error("Invalid arguments", kv("error", e.getMessage()));
            usage();
            System.exit(2);
        } catch (Exception e) {
            log.error("Fatal error", kv("error", e.toString()), e);
            System.exit(1);
        }
    }

    private static int sync(Config config, ObjectMapper mapper, String[] args) throws Exception {
        SyncService.Mode mode = null;
        List<Catalog> catalogs = List.of(Catalog.OWNERSHIP_FORMS, Catalog.COUNTERPARTIES);
        LocalDateTime since = null;
        boolean reset = false;
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "--mode" -> mode = SyncService.Mode.valueOf(value(args, ++i, "--mode").toUpperCase());
                case "--entity" -> {
                    String entity = value(args, ++i, "--entity");
                    if (!entity.equals("all")) {
                        catalogs = List.of(Catalog.byKey(entity));
                    }
                }
                case "--since" -> since = LocalDateTime.parse(value(args, ++i, "--since"));
                case "--reset-state" -> reset = true;
                default -> throw new IllegalArgumentException("Unknown option: " + args[i]);
            }
        }
        if (mode == null) {
            throw new IllegalArgumentException("--mode is required");
        }

        ODataClient odata = new ODataClient(config, mapper);
        StateStore state = new StateStore(config.stateDir(), mapper);
        if (reset) {
            state.reset();
            log.info("Sync state reset");
        }
        try (EventPublisher publisher = new EventPublisher(config, mapper)) {
            List<SyncService.Result> results = new SyncService(config, odata, publisher, state).run(mode, catalogs, since);
            System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(results));
            return 0;
        } catch (Exception e) {
            log.error("Synchronization failed", kv("mode", mode), kv("error", e.toString()), e);
            return 1;
        }
    }

    private static void serve(Config config, ObjectMapper mapper) throws Exception {
        ODataClient odata = new ODataClient(config, mapper);
        HealthServer health = new HealthServer(mapper, odata);
        health.start(config.healthPort());
        log.info("Integration service started", kv("health_port", config.healthPort()),
                kv("odata_url", config.odataUrl()), kv("schedule_interval_s", config.scheduleInterval().toSeconds()));

        CountDownLatch stop = new CountDownLatch(1);
        Runtime.getRuntime().addShutdownHook(new Thread(stop::countDown));

        if (!config.scheduleInterval().isZero()) {
            EventPublisher publisher = new EventPublisher(config, mapper);
            SyncService service = new SyncService(config, odata, publisher, new StateStore(config.stateDir(), mapper));
            ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor();
            scheduler.scheduleWithFixedDelay(() -> {
                try {
                    health.recordSuccess(service.run(SyncService.Mode.INCREMENTAL,
                            List.of(Catalog.OWNERSHIP_FORMS, Catalog.COUNTERPARTIES), null));
                } catch (Exception e) {
                    health.recordFailure(e);
                    log.error("Scheduled sync failed", kv("error", e.toString()));
                }
            }, 10, config.scheduleInterval().toSeconds(), TimeUnit.SECONDS);
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                scheduler.shutdownNow();
                publisher.close();
            }));
        }
        stop.await();
    }

    private static String value(String[] args, int index, String option) {
        if (index >= args.length) {
            throw new IllegalArgumentException(option + " requires a value");
        }
        return args[index];
    }

    private static void usage() {
        System.err.println("""
                Usage:
                  sync --mode full|incremental [--entity all|ownership_forms|counterparties] [--since 2026-07-10T12:00:00] [--reset-state]
                  serve""");
    }
}
