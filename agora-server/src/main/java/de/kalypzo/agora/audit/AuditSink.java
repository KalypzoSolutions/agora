package de.kalypzo.agora.audit;

import de.kalypzo.agora.proto.v1.StoredAuditEntry;
import java.sql.SQLException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Buffers Agora's own audit entries and writes them to Postgres in batches, at the latest every second (E31). The
 * presence never waits on the database: {@link #add} only appends to an in-memory queue and never blocks on I/O. If
 * Postgres is down, the entries stay buffered and are retried; the buffer holds at most {@value #MAX_BUFFERED}
 * entries, the oldest drops (warning at most once a minute).
 */
public final class AuditSink implements AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(AuditSink.class);

    static final int MAX_BUFFERED = 10_000;
    static final int BATCH = 500;
    static final long INTERVAL_MILLIS = 500;
    private static final long WARN_EVERY_NANOS = TimeUnit.MINUTES.toNanos(1);
    private static final long CLOSE_WAIT_SECONDS = 5;

    private final AuditStore store;
    private final Object bufferLock = new Object();
    private final ArrayDeque<StoredAuditEntry> buffer = new ArrayDeque<>(); // guarded by bufferLock
    private final ReentrantLock flushLock = new ReentrantLock(); // one write at a time, never held by add()
    private final ScheduledFuture<?> task;
    private long lastWarn = System.nanoTime() - WARN_EVERY_NANOS; // guarded by bufferLock
    private long dropped; // since the last warning, guarded by bufferLock
    private volatile boolean closed;

    public AuditSink(AuditStore store, ScheduledExecutorService scheduler) {
        this.store = store;
        this.task = scheduler.scheduleWithFixedDelay(this::flushLogged, INTERVAL_MILLIS, INTERVAL_MILLIS,
                TimeUnit.MILLISECONDS);
    }

    /** Queues {@code r} for writing. Never blocks on the database. */
    public void add(StoredAuditEntry r) {
        synchronized (bufferLock) {
            if (buffer.size() >= MAX_BUFFERED) {
                buffer.pollFirst();
                dropped++;
                warnDropped();
            }
            buffer.addLast(r);
        }
    }

    private void warnDropped() {
        long now = System.nanoTime();
        if (now - lastWarn >= WARN_EVERY_NANOS) {
            LOG.warn("Audit buffer is full ({} entries), dropped {} oldest entries", MAX_BUFFERED, dropped);
            lastWarn = now;
            dropped = 0;
        }
    }

    /** Writes everything buffered so far (in batches). A failed batch stays buffered for the next attempt. */
    public void flush() {
        flushLock.lock();
        try {
            drain();
        } finally {
            flushLock.unlock();
        }
    }

    private void drain() {
        while (true) {
            List<StoredAuditEntry> batch;
            synchronized (bufferLock) {
                if (buffer.isEmpty()) return;
                batch = new ArrayList<>(Math.min(BATCH, buffer.size()));
                while (batch.size() < BATCH && !buffer.isEmpty()) batch.add(buffer.pollFirst());
            }
            try {
                store.insert(batch);
            } catch (SQLException | RuntimeException e) {
                LOG.warn("Writing {} audit entries failed, will retry: {}", batch.size(), e.toString());
                requeue(batch);
                return;
            }
        }
    }

    private void requeue(List<StoredAuditEntry> batch) {
        synchronized (bufferLock) {
            for (int i = batch.size() - 1; i >= 0; i--) {
                if (buffer.size() >= MAX_BUFFERED) { // the oldest (this batch) drop first
                    dropped += i + 1;
                    warnDropped();
                    return;
                }
                buffer.addFirst(batch.get(i));
            }
        }
    }

    private void flushLogged() {
        try {
            flush();
        } catch (RuntimeException e) {
            LOG.error("Audit flush failed", e);
        }
    }

    /** Stops the timer and writes what is left (one attempt, waits at most 5 s for a running write). */
    @Override
    public void close() {
        if (closed) return;
        closed = true;
        task.cancel(false);
        try {
            if (flushLock.tryLock(CLOSE_WAIT_SECONDS, TimeUnit.SECONDS)) {
                try {
                    drain();
                } finally {
                    flushLock.unlock();
                }
            } else {
                LOG.warn("Audit sink: a write is still running at shutdown, buffered entries are lost");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
