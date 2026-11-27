package de.kalypzo.agora.sql;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * A piece of SQL text with its bound parameters, in the order of their {@code ?} placeholders.
 *
 * <p>Parameters are plain JDBC values ({@code Long}, {@code Double}, {@code Boolean}, {@code String}); JSON values are
 * bound as {@code String} next to a {@code ?::jsonb} cast.
 */
public record SqlFragment(String sql, List<Object> params) {

    public SqlFragment {
        Objects.requireNonNull(sql, "sql");
        params = List.copyOf(params);
    }

    public static SqlFragment of(String sql) {
        return new SqlFragment(sql, List.of());
    }

    public static SqlFragment of(String sql, Object param) {
        return new SqlFragment(sql, List.of(param));
    }

    /** Concatenates fragments; parameters keep their order. */
    public static SqlFragment concat(SqlFragment... parts) {
        StringBuilder sql = new StringBuilder();
        List<Object> params = new ArrayList<>();
        for (SqlFragment p : parts) {
            sql.append(p.sql);
            params.addAll(p.params);
        }
        return new SqlFragment(sql.toString(), params);
    }

    /** Joins fragments with a separator; parameters keep their order. */
    public static SqlFragment join(String separator, List<SqlFragment> parts) {
        StringBuilder sql = new StringBuilder();
        List<Object> params = new ArrayList<>();
        for (int i = 0; i < parts.size(); i++) {
            if (i > 0) sql.append(separator);
            sql.append(parts.get(i).sql);
            params.addAll(parts.get(i).params);
        }
        return new SqlFragment(sql.toString(), params);
    }
}
