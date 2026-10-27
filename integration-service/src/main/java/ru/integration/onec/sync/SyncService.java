package ru.integration.onec.sync;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.integration.onec.Config;
import ru.integration.onec.kafka.EventPublisher;
import ru.integration.onec.odata.ODataClient;

import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static net.logstash.logback.argument.StructuredArguments.kv;

/**
 * Полная и инкрементальная синхронизация справочников 1С в Kafka.
 * Формы собственности выгружаются раньше контрагентов, т.к. контрагенты на них ссылаются.
 */
public class SyncService {

    public enum Mode { FULL, INCREMENTAL }

    public record Result(String entity, Mode mode, LocalDateTime changedSince, int read, int published,
                         int skipped, LocalDateTime watermark, long durationMs) {
    }

    private static final Logger log = LoggerFactory.getLogger(SyncService.class);

    private final Config config;
    private final ODataClient odata;
    private final EventPublisher publisher;
    private final StateStore state;

    public SyncService(Config config, ODataClient odata, EventPublisher publisher, StateStore state) {
        this.config = config;
        this.odata = odata;
        this.publisher = publisher;
        this.state = state;
    }

    /**
     * @param since явная метка для инкрементального режима (перекрывает сохранённую), может быть null
     */
    public List<Result> run(Mode mode, List<Catalog> catalogs, LocalDateTime since) throws Exception {
        Files.createDirectories(config.stateDir());
        // Защита от параллельного запуска (фоновое расписание + ручной `docker compose exec ... sync`)
        try (FileChannel channel = FileChannel.open(config.stateDir().resolve("sync.lock"),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE);
             FileLock lock = channel.tryLock()) {
            if (lock == null) {
                throw new IllegalStateException("Another synchronization is already running");
            }
            List<Result> results = new ArrayList<>();
            for (Catalog catalog : catalogs) {
                results.add(syncCatalog(mode, catalog, since));
            }
            return results;
        }
    }

    private Result syncCatalog(Mode mode, Catalog catalog, LocalDateTime since) throws Exception {
        Instant started = Instant.now();
        LocalDateTime previous = state.watermark(catalog);
        LocalDateTime changedSince = null;
        if (mode == Mode.INCREMENTAL) {
            if (since != null) {
                changedSince = since;
            } else if (previous != null) {
                // небольшое перекрытие на случай записей, закоммиченных в 1С позже своей метки;
                // дубликаты безопасны — запись в PostgreSQL идемпотентна
                changedSince = previous.minus(config.incrementalOverlap());
            } else {
                log.warn("No saved watermark, incremental sync falls back to full load", kv("entity", catalog.key));
            }
        }
        log.info("Sync started", kv("entity", catalog.key), kv("mode", mode),
                kv("changed_since", String.valueOf(changedSince)), kv("topic", catalog.topic(config)));

        int[] counters = new int[3]; // read, published, skipped
        LocalDateTime[] max = {previous};
        String topic = catalog.topic(config);

        odata.readAll(catalog.entitySet, catalog.select, changedSince, page -> {
            for (JsonNode row : page) {
                counters[0]++;
                ObjectNode payload;
                try {
                    payload = catalog.toPayload(row);
                } catch (RuntimeException e) {
                    counters[2]++;
                    log.error("Skipping invalid 1C record", kv("entity", catalog.key),
                            kv("record", row.toString()), kv("error", e.toString()));
                    continue;
                }
                try {
                    publisher.publish(topic, catalog.eventType, payload);
                } catch (Exception e) {
                    throw new IllegalStateException("Failed to publish event to " + topic + ": " + e, e);
                }
                counters[1]++;
                LocalDateTime updated = Catalog.updatedAt(row);
                if (updated != null && (max[0] == null || updated.isAfter(max[0]))) {
                    max[0] = updated;
                }
            }
        });
        publisher.flush();
        // метку двигаем только после того, как Kafka подтвердила все сообщения
        if (max[0] != null) {
            state.saveWatermark(catalog, max[0]);
        }

        Result result = new Result(catalog.key, mode, changedSince, counters[0], counters[1], counters[2],
                max[0], Duration.between(started, Instant.now()).toMillis());
        log.info("Sync finished", kv("entity", result.entity()), kv("mode", mode), kv("read", result.read()),
                kv("published", result.published()), kv("skipped", result.skipped()),
                kv("watermark", String.valueOf(result.watermark())), kv("duration_ms", result.durationMs()));
        return result;
    }
}
