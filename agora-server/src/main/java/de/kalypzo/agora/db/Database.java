package de.kalypzo.agora.db;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import de.kalypzo.agora.config.AgoraConfig;

/** Creates the Hikari pool. */
public final class Database {

    private Database() {}

    public static HikariDataSource open(AgoraConfig config) {
        HikariConfig cfg = new HikariConfig();
        cfg.setPoolName("agora");
        cfg.setJdbcUrl(config.dbUrl());
        cfg.setUsername(config.dbUser());
        cfg.setPassword(config.dbPassword());
        cfg.setMaximumPoolSize(config.dbPoolSize());
        cfg.setConnectionTimeout(5_000); // no request may wait longer than the client deadline (5 s)
        return new HikariDataSource(cfg);
    }
}
