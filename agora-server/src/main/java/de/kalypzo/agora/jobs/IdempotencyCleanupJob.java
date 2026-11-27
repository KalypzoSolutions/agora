package de.kalypzo.agora.jobs;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import javax.sql.DataSource;

/**
 * Deletes idempotency keys older than {@link #RETENTION} (agora §5). Runs every {@link #INTERVAL}. Deletes in
 * {@link #BATCH_SIZE}-bounded statements until a batch comes back short (like {@link ExpireJob}), so a backlog never
 * needs one statement longer than the query timeout and every committed batch is progress.
 */
public final class IdempotencyCleanupJob implements Runnable {

    public static final Duration RETENTION = Duration.ofHours(24);
    public static final Duration INTERVAL = Duration.ofHours(1);

    /** Rows deleted per statement (each its own transaction). */
    public static final int BATCH_SIZE = 10_000;

    private static final int QUERY_TIMEOUT_SECONDS = 5;

    private final DataSource ds;
    private final Clock clock;
    private final int batchSize;

    public IdempotencyCleanupJob(DataSource ds, Clock clock) {
        this(ds, clock, BATCH_SIZE);
    }

    IdempotencyCleanupJob(DataSource ds, Clock clock, int batchSize) {
        this.ds = ds;
        this.clock = clock;
        this.batchSize = batchSize;
    }

    @Override
    public void run() {
        Instant cutoff = clock.instant().minus(RETENTION);
        int deleted;
        do {
            deleted = deleteBatch(cutoff);
        } while (deleted >= batchSize);
    }

    private int deleteBatch(Instant cutoff) {
        try (Connection c = ds.getConnection();
                PreparedStatement ps = c.prepareStatement("DELETE FROM pantheon_idempotency WHERE ctid IN"
                        + " (SELECT ctid FROM pantheon_idempotency WHERE created_at < ? LIMIT ?)")) {
            ps.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
            ps.setTimestamp(1, Timestamp.from(cutoff));
            ps.setInt(2, batchSize);
            return ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("idempotency cleanup failed", e);
        }
    }
}
