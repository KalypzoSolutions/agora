package de.kalypzo.agora.jobs;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.Clock;
import java.time.Duration;
import javax.sql.DataSource;

/** Deletes idempotency keys older than {@link #RETENTION} (agora §5). Runs every {@link #INTERVAL}. */
public final class IdempotencyCleanupJob implements Runnable {

    public static final Duration RETENTION = Duration.ofHours(24);
    public static final Duration INTERVAL = Duration.ofHours(1);

    private static final int QUERY_TIMEOUT_SECONDS = 5;

    private final DataSource ds;
    private final Clock clock;

    public IdempotencyCleanupJob(DataSource ds, Clock clock) {
        this.ds = ds;
        this.clock = clock;
    }

    @Override
    public void run() {
        try (Connection c = ds.getConnection();
                PreparedStatement ps = c.prepareStatement("DELETE FROM pantheon_idempotency WHERE created_at < ?")) {
            ps.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
            ps.setTimestamp(1, Timestamp.from(clock.instant().minus(RETENTION)));
            ps.executeUpdate();
        } catch (SQLException e) {
            throw new IllegalStateException("idempotency cleanup failed", e);
        }
    }
}
