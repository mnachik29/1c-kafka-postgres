package ru.integration.onec.sync;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.integration.onec.Config;
import ru.integration.onec.kafka.EventPublisher;
import ru.integration.onec.odata.ODataClient;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.OverlappingFileLockException;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static net.logstash.logback.argument.StructuredArguments.kv;

/**
 * Полная и инкрементальная синхронизация справочников 1С в Kafka.
 * Формы собственности выгружаются раньше контрагентов, т.к. контрагенты на них ссылаются.
 */
public class SyncService {

    public enum Mode { FULL, INCREMENTAL }

    /**
     * @param skipped   невалидные записи 1С (не опубликованы)
     * @param unchanged записи на границе окна, чья версия уже в Kafka (не опубликованы повторно)
     */
    public record Result(String entity, Mode mode, LocalDateTime changedSince, int read, int published,
                         int skipped, int unchanged, LocalDateTime watermark, long durationMs) {
    }

    /** Синхронизация уже идёт (по расписанию, через CLI или HTTP). */
    public static class AlreadyRunningException extends IllegalStateException {
        public AlreadyRunningException() {
            super("Another synchronization is already running");
        }
    }

    private static final Logger log = LoggerFactory.getLogger(SyncService.class);

    private final Config config;
    private final ODataClient odata;
    private final EventPublisher publisher;
    private final StateStore state;
    private final SyncStats stats = new SyncStats();

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
        return run(mode, catalogs, since, false);
    }

    /**
     * @param resetState сбросить сохранённые метки перед запуском (под той же блокировкой)
     */
    public List<Result> run(Mode mode, List<Catalog> catalogs, LocalDateTime since, boolean resetState)
            throws Exception {
        Files.createDirectories(config.stateDir());
        // Защита от параллельного запуска: расписание, POST /sync и `docker compose exec ... sync`
        try (FileChannel channel = FileChannel.open(config.stateDir().resolve("sync.lock"),
                StandardOpenOption.CREATE, StandardOpenOption.WRITE);
             FileLock lock = tryLock(channel)) {
            if (lock == null) {
                throw new AlreadyRunningException();
            }
            try {
                if (resetState) {
                    state.reset();
                    log.info("Sync state reset");
                }
                List<Result> results = new ArrayList<>();
                for (Catalog catalog : catalogs) {
                    results.add(syncCatalog(mode, catalog, since));
                }
                stats.recordSuccess(results);
                return results;
            } catch (Exception e) {
                stats.recordFailure(e);
                log.error("Synchronization failed", kv("mode", mode), kv("error", e.toString()), e);
                throw e;
            }
        }
    }

    public SyncStats stats() {
        return stats;
    }

    /** "all" или null — оба справочника, иначе один по ключу. */
    public static List<Catalog> catalogs(String entity) {
        return entity == null || entity.equals("all")
                ? List.of(Catalog.OWNERSHIP_FORMS, Catalog.COUNTERPARTIES)
                : List.of(Catalog.byKey(entity));
    }

    private static FileLock tryLock(FileChannel channel) throws IOException {
        try {
            return channel.tryLock();
        } catch (OverlappingFileLockException e) {
            return null; // блокировку уже держит другой поток этого же процесса
        }
    }

    private Result syncCatalog(Mode mode, Catalog catalog, LocalDateTime since) throws Exception {
        Instant started = Instant.now();
        StateStore.Checkpoint previous = state.checkpoint(catalog);
        LocalDateTime changedSince = null;
        // уже отправленные версии на границе окна пропускаем только в обычном инкременте;
        // full и явный since отправляют всё
        Map<String, String> alreadySent = Map.of();
        if (mode == Mode.INCREMENTAL) {
            if (since != null) {
                changedSince = since;
            } else if (previous.watermark() != null) {
                // небольшое перекрытие на случай записей, закоммиченных в 1С позже своей метки
                changedSince = previous.watermark().minus(config.incrementalOverlap());
                alreadySent = previous.boundary();
            } else {
                log.warn("No saved watermark, incremental sync falls back to full load", kv("entity", catalog.key));
            }
        }
        log.info("Sync started", kv("entity", catalog.key), kv("mode", mode),
                kv("changed_since", String.valueOf(changedSince)), kv("topic", catalog.topic(config)));

        int[] counters = new int[4]; // read, published, skipped, unchanged
        LocalDateTime[] max = {previous.watermark()};
        Map<String, LocalDateTime> seenAt = new HashMap<>();
        Map<String, String> seenVersion = new HashMap<>();
        Map<String, String> sent = alreadySent;
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
                String id = payload.get("id").asText();
                String version = payload.path("data_version").asText(null);
                LocalDateTime updated = Catalog.updatedAt(row);
                if (updated != null && version != null) {
                    seenAt.put(id, updated);
                    seenVersion.put(id, version);
                }
                if (updated != null && (max[0] == null || updated.isAfter(max[0]))) {
                    max[0] = updated;
                }
                if (version != null && version.equals(sent.get(id))) {
                    counters[3]++; // эта версия уже в Kafka — попала повторно из-за перекрытия окна
                    continue;
                }
                try {
                    publisher.publish(topic, catalog.eventType, payload);
                } catch (Exception e) {
                    throw new IllegalStateException("Failed to publish event to " + topic + ": " + e, e);
                }
                counters[1]++;
            }
        });
        publisher.flush();
        // метку и границу окна сохраняем только после того, как Kafka подтвердила все сообщения
        if (max[0] != null) {
            state.save(catalog, new StateStore.Checkpoint(max[0], boundary(max[0], seenAt, seenVersion)));
        }

        Result result = new Result(catalog.key, mode, changedSince, counters[0], counters[1], counters[2],
                counters[3], max[0], Duration.between(started, Instant.now()).toMillis());
        log.info("Sync finished", kv("entity", result.entity()), kv("mode", mode), kv("read", result.read()),
                kv("published", result.published()), kv("skipped", result.skipped()),
                kv("unchanged", result.unchanged()), kv("watermark", String.valueOf(result.watermark())),
                kv("duration_ms", result.durationMs()));
        return result;
    }

    /**
     * Версии записей, которые следующий инкремент прочитает повторно (ДатаИзменения в пределах перекрытия
     * от новой метки). Все такие записи прочитаны в этом прогоне: новое окно начинается не раньше старого.
     */
    private Map<String, String> boundary(LocalDateTime watermark, Map<String, LocalDateTime> seenAt,
                                         Map<String, String> seenVersion) {
        LocalDateTime windowStart = watermark.minus(config.incrementalOverlap());
        Map<String, String> boundary = new HashMap<>();
        seenAt.forEach((id, at) -> {
            if (!at.isBefore(windowStart)) {
                boundary.put(id, seenVersion.get(id));
            }
        });
        return boundary;
    }
}
