package ru.integration.sink;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.integration.sink.db.CatalogRepository;
import ru.integration.sink.kafka.DlqPublisher;
import ru.integration.sink.model.CatalogEvent;
import ru.integration.sink.model.EventParser;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import static net.logstash.logback.argument.StructuredArguments.kv;

/**
 * Обработка пакета из poll():
 * <ol>
 *   <li>невалидные сообщения сразу уходят в DLQ;</li>
 *   <li>валидные пишутся одной транзакцией, временные ошибки БД повторяются с backoff;</li>
 *   <li>если пакет падает из-за данных (не из-за доступности БД) — пишем по одному,
 *       «плохие» сообщения уходят в DLQ, остальные сохраняются;</li>
 *   <li>если БД недоступна — бросаем исключение, offset не коммитится, пакет будет перечитан.</li>
 * </ol>
 */
public class BatchProcessor {

    /** БД недоступна: пакет нужно перечитать позже, коммитить offset нельзя. */
    public static class DatabaseUnavailableException extends Exception {
        public DatabaseUnavailableException(String message, Throwable cause) {
            super(message, cause);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(BatchProcessor.class);

    private final Config config;
    private final EventParser parser;
    private final CatalogRepository repository;
    private final DlqPublisher dlq;
    private final Metrics metrics;

    public BatchProcessor(Config config, EventParser parser, CatalogRepository repository, DlqPublisher dlq,
                          Metrics metrics) {
        this.config = config;
        this.parser = parser;
        this.repository = repository;
        this.dlq = dlq;
        this.metrics = metrics;
    }

    public void process(ConsumerRecords<String, String> records) throws Exception {
        List<CatalogEvent> events = new ArrayList<>(records.count());
        List<ConsumerRecord<String, String>> sources = new ArrayList<>(records.count());
        for (ConsumerRecord<String, String> record : records) {
            try {
                events.add(parser.parse(record.value()));
                sources.add(record);
            } catch (EventParser.InvalidEventException e) {
                metrics.invalid.incrementAndGet();
                dlq.send(record, "invalid event: " + e.getMessage());
            }
        }

        if (!events.isEmpty()) {
            try {
                int changed = writeWithRetry(events);
                metrics.written.addAndGet(events.size());
                metrics.rowsChanged.addAndGet(changed);
                log.info("Batch written", kv("messages", records.count()), kv("valid", events.size()),
                        kv("rows_changed", changed));
            } catch (SQLException batchError) {
                log.warn("Batch failed on data error, falling back to one-by-one processing",
                        kv("messages", events.size()), kv("sql_state", batchError.getSQLState()),
                        kv("error", batchError.getMessage()));
                writeOneByOne(events, sources);
            }
        }
        dlq.flush();
        metrics.batches.incrementAndGet();
    }

    private void writeOneByOne(List<CatalogEvent> events, List<ConsumerRecord<String, String>> sources)
            throws Exception {
        for (int i = 0; i < events.size(); i++) {
            try {
                metrics.rowsChanged.addAndGet(writeWithRetry(List.of(events.get(i))));
                metrics.written.incrementAndGet();
            } catch (SQLException e) {
                metrics.dbErrors.incrementAndGet();
                log.error("Failed to write message to PostgreSQL", kv("topic", sources.get(i).topic()),
                        kv("offset", sources.get(i).offset()), kv("key", sources.get(i).key()),
                        kv("sql_state", e.getSQLState()), kv("error", e.getMessage()));
                dlq.send(sources.get(i), "postgres error " + e.getSQLState() + ": " + e.getMessage());
            }
        }
    }

    /**
     * Повторяет только временные ошибки (нет соединения, deadlock/serialization).
     * Ошибки данных (нарушение ограничений и т.п.) пробрасываются сразу как SQLException.
     */
    private int writeWithRetry(List<CatalogEvent> events) throws SQLException, DatabaseUnavailableException,
            InterruptedException {
        long backoff = config.retryInitialBackoff().toMillis();
        for (int attempt = 1; ; attempt++) {
            try {
                return repository.write(events);
            } catch (SQLException e) {
                if (!isTransient(e)) {
                    throw e;
                }
                metrics.dbErrors.incrementAndGet();
                if (attempt >= config.retryMaxAttempts()) {
                    throw new DatabaseUnavailableException("PostgreSQL is unavailable after " + attempt + " attempts", e);
                }
                log.warn("Transient PostgreSQL error, will retry", kv("attempt", attempt),
                        kv("backoff_ms", backoff), kv("sql_state", e.getSQLState()), kv("error", e.getMessage()));
                Thread.sleep(backoff);
                backoff = Math.min(backoff * 2, 30_000);
            }
        }
    }

    static boolean isTransient(SQLException e) {
        String state = e.getSQLState();
        if (state == null) {
            return true; // нет SQLSTATE — как правило, проблема соединения/пула
        }
        return state.startsWith("08")      // connection exception
                || state.startsWith("40")  // transaction rollback: deadlock, serialization failure
                || state.startsWith("53")  // insufficient resources
                || state.startsWith("57"); // operator intervention (shutdown, cancel)
    }
}
