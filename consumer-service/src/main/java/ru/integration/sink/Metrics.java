package ru.integration.sink;

import java.util.concurrent.atomic.AtomicLong;

/** Простые счётчики для /metrics. */
public class Metrics {
    public final AtomicLong batches = new AtomicLong();
    public final AtomicLong written = new AtomicLong();
    public final AtomicLong rowsChanged = new AtomicLong();
    public final AtomicLong invalid = new AtomicLong();
    public final AtomicLong dbErrors = new AtomicLong();
    public final AtomicLong restarts = new AtomicLong();
    public volatile long lastPollAtMillis = System.currentTimeMillis();

    public String prometheus() {
        return "sink_batches_total " + batches.get() + "\n"
                + "sink_messages_written_total " + written.get() + "\n"
                + "sink_rows_changed_total " + rowsChanged.get() + "\n"
                + "sink_messages_invalid_total " + invalid.get() + "\n"
                + "sink_db_errors_total " + dbErrors.get() + "\n"
                + "sink_consumer_restarts_total " + restarts.get() + "\n";
    }
}
