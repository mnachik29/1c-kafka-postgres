package ru.integration.onec.sync;

import java.time.Instant;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/** Итог последнего запуска и счётчики синхронизаций — для /health и /metrics. */
public class SyncStats {

    /** Снимок последнего запуска; {@code at == null} — запусков ещё не было. */
    public record LastRun(Instant at, List<SyncService.Result> results, String error) {
    }

    private final AtomicReference<LastRun> lastRun = new AtomicReference<>(new LastRun(null, List.of(), null));
    private final AtomicLong runsOk = new AtomicLong();
    private final AtomicLong runsFailed = new AtomicLong();
    private final AtomicLong eventsPublished = new AtomicLong();

    void recordSuccess(List<SyncService.Result> results) {
        lastRun.set(new LastRun(Instant.now(), results, null));
        runsOk.incrementAndGet();
        results.forEach(r -> eventsPublished.addAndGet(r.published()));
    }

    void recordFailure(Exception e) {
        lastRun.set(new LastRun(Instant.now(), List.of(), e.toString()));
        runsFailed.incrementAndGet();
    }

    public LastRun lastRun() {
        return lastRun.get();
    }

    public long runsOk() {
        return runsOk.get();
    }

    public long runsFailed() {
        return runsFailed.get();
    }

    public long eventsPublished() {
        return eventsPublished.get();
    }
}
