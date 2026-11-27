package de.kalypzo.agora;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import de.kalypzo.agora.db.Migrations;
import java.sql.Connection;
import java.sql.ResultSet;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;

/** A-16 / E39: the partitions exist when the job is handed to the scheduler, i.e. before health can be SERVING. */
class AuditPartitionWiringTest {

    @Test
    void partitionsExistBeforeJobIsScheduled() throws Exception {
        try (PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine")) {
            postgres.start();
            HikariConfig cfg = new HikariConfig();
            cfg.setJdbcUrl(postgres.getJdbcUrl());
            cfg.setUsername(postgres.getUsername());
            cfg.setPassword(postgres.getPassword());
            try (HikariDataSource ds = new HikariDataSource(cfg)) {
                Migrations.migrate(ds);
                AtomicInteger countAtSchedule = new AtomicInteger(-1);
                ScheduledThreadPoolExecutor scheduler = new ScheduledThreadPoolExecutor(1) {
                    @Override
                    public ScheduledFuture<?> scheduleWithFixedDelay(Runnable r, long i, long d, TimeUnit u) {
                        countAtSchedule.set(count(ds));
                        return null;
                    }
                };
                try {
                    AgoraServer.scheduleAuditPartitions(scheduler, ds);
                } finally {
                    scheduler.shutdownNow();
                }
                assertThat(countAtSchedule.get()).isEqualTo(3);
            }
        }
    }

    private static int count(HikariDataSource ds) {
        try (Connection c = ds.getConnection();
                var st = c.createStatement();
                ResultSet rs = st.executeQuery(
                        "SELECT count(*) FROM pg_inherits WHERE inhparent = 'pantheon_audit'::regclass")) {
            rs.next();
            return rs.getInt(1);
        } catch (java.sql.SQLException e) {
            throw new IllegalStateException(e);
        }
    }
}
