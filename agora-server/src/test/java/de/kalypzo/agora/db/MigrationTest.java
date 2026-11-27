package de.kalypzo.agora.db;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.postgresql.PostgreSQLContainer;

class MigrationTest {

    static PostgreSQLContainer postgres = new PostgreSQLContainer("postgres:17-alpine");
    static HikariDataSource ds;

    @BeforeAll
    static void start() {
        postgres.start();
        HikariConfig cfg = new HikariConfig();
        cfg.setJdbcUrl(postgres.getJdbcUrl());
        cfg.setUsername(postgres.getUsername());
        cfg.setPassword(postgres.getPassword());
        cfg.setMaximumPoolSize(4);
        ds = new HikariDataSource(cfg);
        Migrations.migrate(ds);
    }

    @AfterAll
    static void stop() {
        ds.close();
        postgres.stop();
    }

    private static String scalar(String sql) throws SQLException {
        try (Connection c = ds.getConnection();
                var st = c.createStatement();
                ResultSet rs = st.executeQuery(sql)) {
            rs.next();
            return rs.getString(1);
        }
    }

    private static void exec(String sql) throws SQLException {
        try (Connection c = ds.getConnection(); var st = c.createStatement()) {
            st.execute(sql);
        }
    }

    @Test
    void createsAllTables() throws SQLException {
        List<String> tables = new ArrayList<>();
        try (Connection c = ds.getConnection();
                var st = c.createStatement();
                ResultSet rs = st.executeQuery(
                        "SELECT tablename FROM pg_tables WHERE tablename LIKE 'pantheon\\_%'")) {
            while (rs.next()) tables.add(rs.getString(1));
        }
        assertThat(tables)
                .contains("pantheon_player", "pantheon_doc", "pantheon_idempotency", "pantheon_index");
    }

    @Test
    void setAddIsIdempotent() throws SQLException {
        assertThat(scalar("SELECT pantheon_set_add('{\"kits\":[\"a\"]}'::jsonb, 'kits', '\"a\"'::jsonb)::text"))
                .isEqualTo("{\"kits\": [\"a\"]}");
        assertThat(scalar(
                        "SELECT pantheon_set_add('{\"kits\":[\"a\"]}'::jsonb, 'kits', '\"a\"'::jsonb) = '{\"kits\":[\"a\"]}'::jsonb"))
                .isEqualTo("t");
    }

    @Test
    void setAddCreatesMissingArray() throws SQLException {
        assertThat(scalar("SELECT pantheon_set_add('{}'::jsonb, 'kits', '\"a\"'::jsonb) = '{\"kits\":[\"a\"]}'::jsonb"))
                .isEqualTo("t");
    }

    @Test
    void setAddAppendsNewElement() throws SQLException {
        assertThat(scalar(
                        "SELECT pantheon_set_add('{\"kits\":[\"a\"]}'::jsonb, 'kits', '\"b\"'::jsonb) = '{\"kits\":[\"a\",\"b\"]}'::jsonb"))
                .isEqualTo("t");
    }

    @Test
    void setRemoveRemovesAllOccurrences() throws SQLException {
        assertThat(scalar(
                        "SELECT pantheon_set_remove('{\"kits\":[\"a\",\"b\",\"a\"]}'::jsonb, 'kits', '\"a\"'::jsonb) = '{\"kits\":[\"b\"]}'::jsonb"))
                .isEqualTo("t");
    }

    @Test
    void setRemoveOnMissingArrayIsNoop() throws SQLException {
        assertThat(scalar("SELECT pantheon_set_remove('{}'::jsonb, 'kits', '\"a\"'::jsonb) = '{}'::jsonb"))
                .isEqualTo("t");
    }

    @Test
    void versionSequenceIncreasesAcrossTables() throws SQLException {
        exec("INSERT INTO pantheon_player (player, namespace) VALUES (gen_random_uuid(), 'v')");
        exec("INSERT INTO pantheon_doc (store, id) VALUES ('v:s', 'x')");
        exec("INSERT INTO pantheon_player (player, namespace) VALUES (gen_random_uuid(), 'v2')");
        long p1 = Long.parseLong(scalar("SELECT version FROM pantheon_player WHERE namespace = 'v'"));
        long d = Long.parseLong(scalar("SELECT version FROM pantheon_doc WHERE store = 'v:s'"));
        long p2 = Long.parseLong(scalar("SELECT version FROM pantheon_player WHERE namespace = 'v2'"));
        assertThat(d).isGreaterThan(p1);
        assertThat(p2).isGreaterThan(d);
    }
}
