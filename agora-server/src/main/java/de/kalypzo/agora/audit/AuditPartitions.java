package de.kalypzo.agora.audit;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Daily (UTC) partitions {@code pantheon_audit_YYYYMMDD} of {@code pantheon_audit} (audit_log §9, E39). */
public final class AuditPartitions {

    private static final Logger LOG = LoggerFactory.getLogger(AuditPartitions.class);
    private static final String PREFIX = "pantheon_audit_";
    private static final DateTimeFormatter DAY = DateTimeFormatter.BASIC_ISO_DATE;

    /** Advisory lock key serialising ensure/dropExpired across threads and Agora instances. */
    private static final long LOCK_KEY = 0x70616e5f61756431L;

    private final DataSource ds;
    private final Clock clock;

    /** Upper bound for waiting on the advisory lock (no thread waits without a deadline). */
    public static final Duration LOCK_TIMEOUT = Duration.ofSeconds(10);

    private final Duration lockTimeout;

    public AuditPartitions(DataSource ds, Clock clock) {
        this(ds, clock, LOCK_TIMEOUT);
    }

    public AuditPartitions(DataSource ds, Clock clock, Duration lockTimeout) {
        this.ds = ds;
        this.clock = clock;
        this.lockTimeout = lockTimeout;
    }

    /** Must run before {@code pg_advisory_xact_lock}: lock_timeout also bounds the advisory-lock wait. */
    private void lock(Statement st) throws SQLException {
        st.execute("SET LOCAL lock_timeout = '" + Math.max(1, lockTimeout.toMillis()) + "ms'");
        st.execute("SELECT pg_advisory_xact_lock(" + LOCK_KEY + ")");
    }

    /**
     * Creates the missing partitions for yesterday ... today + {@code daysAhead} (UTC). Yesterday is included (R54)
     * because Agora accepts entries up to 24 h old, which on a fresh Agora would otherwise hit a missing partition.
     */
    public void ensure(int daysAhead) {
        LocalDate today = LocalDate.ofInstant(clock.instant(), ZoneOffset.UTC);
        try (Connection c = ds.getConnection(); Statement st = c.createStatement()) {
            c.setAutoCommit(false);
            lock(st);
            for (int i = -1; i <= daysAhead; i++) {
                LocalDate day = today.plusDays(i);
                st.execute("CREATE TABLE IF NOT EXISTS " + PREFIX + DAY.format(day)
                        + " PARTITION OF pantheon_audit FOR VALUES FROM ('" + bound(day) + "') TO ('"
                        + bound(day.plusDays(1)) + "')");
            }
            c.commit();
        } catch (SQLException e) {
            throw new IllegalStateException("creating audit partitions failed", e);
        }
    }

    /** Drops every partition whose day ends at or before {@code now - retention}. */
    public void dropExpired(Duration retention) {
        Instant cutoff = clock.instant().minus(retention);
        try (Connection c = ds.getConnection(); Statement st = c.createStatement()) {
            c.setAutoCommit(false);
            lock(st);
            for (String name : partitionNames(st)) {
                LocalDate day;
                try {
                    day = LocalDate.parse(name.substring(PREFIX.length()), DAY);
                } catch (RuntimeException e) {
                    continue; // not one of ours
                }
                if (!day.plusDays(1).atStartOfDay().toInstant(ZoneOffset.UTC).isAfter(cutoff)) {
                    st.execute("DROP TABLE IF EXISTS " + name);
                    LOG.info("Dropped expired audit partition {}", name);
                }
            }
            c.commit();
        } catch (SQLException e) {
            throw new IllegalStateException("dropping audit partitions failed", e);
        }
    }

    private static List<String> partitionNames(Statement st) throws SQLException {
        List<String> names = new ArrayList<>();
        try (ResultSet rs = st.executeQuery("SELECT c.relname FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid"
                + " WHERE i.inhparent = 'pantheon_audit'::regclass")) {
            while (rs.next()) {
                String n = rs.getString(1);
                if (n.startsWith(PREFIX)) names.add(n);
            }
        }
        return names;
    }

    private static String bound(LocalDate day) {
        return UuidV7.lowerBound(day.atStartOfDay().toInstant(ZoneOffset.UTC)).toString();
    }
}
