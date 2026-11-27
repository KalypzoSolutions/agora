package de.kalypzo.agora.db;

import javax.sql.DataSource;
import org.flywaydb.core.Flyway;

/** Runs the Flyway migrations from {@code db/migration}. */
public final class Migrations {

    private Migrations() {}

    public static void migrate(DataSource dataSource) {
        Flyway.configure()
                .dataSource(dataSource)
                .locations("classpath:db/migration")
                .load()
                .migrate();
    }
}
