package de.kalypzo.agora.sql;

import de.kalypzo.agora.proto.v1.FieldSpec;
import de.kalypzo.agora.proto.v1.ScalarType;

/**
 * Typed read access to a field, with its default (agora.md §5).
 *
 * <ul>
 *   <li>LONG {@code COALESCE((a.data->>'k')::bigint, '5'::bigint)}, DOUBLE {@code ::double precision}, BOOLEAN
 *       {@code ::boolean}
 *   <li>STRING {@code COALESCE((a.data->>'k'), 'x') COLLATE "C"}
 *   <li>LIST/SET/OBJECT {@code COALESCE(a.data->'k', '[]'::jsonb)}
 * </ul>
 *
 * <p>Without a default there is no {@code COALESCE}. Defaults are typed literals (ruling R4), so the fragment has no
 * parameters and is usable verbatim as an index expression; an empty alias reads the bare {@code data} column.
 */
public final class FieldExpr {

    private FieldExpr() {}

    public static SqlFragment read(String alias, FieldSpec f) {
        String name = SqlLiteral.fieldName(f.getName());
        String data = SqlLiteral.dataColumn(alias);
        if (Values.isJsonType(f.getType())) {
            return withDefault(SqlFragment.of("(" + data + "->'" + name + "')"), f);
        }
        return typed(SqlFragment.of("(" + data + "->>'" + name + "')"), f);
    }

    /** {@code text} (an SQL text expression) cast to the field's type, with default and, for STRING, collation. */
    static SqlFragment typed(SqlFragment text, FieldSpec f) {
        String cast = switch (f.getType()) {
            case LONG -> "::bigint";
            case DOUBLE -> "::double precision";
            case BOOLEAN -> "::boolean";
            case STRING -> "";
            default -> throw new IllegalArgumentException("not a scalar type: " + f.getType());
        };
        SqlFragment expr = withDefault(SqlFragment.concat(text, SqlFragment.of(cast)), f);
        return f.getType() == ScalarType.STRING ? SqlFragment.concat(expr, SqlFragment.of(" COLLATE \"C\"")) : expr;
    }

    /** {@code COALESCE(expr, <default literal>)}, or {@code expr} if the field has no default. */
    static SqlFragment withDefault(SqlFragment expr, FieldSpec f) {
        if (!f.hasDefaultValue() || Values.isNull(f.getDefaultValue())) {
            SqlLiteral.sqlType(f.getType()); // rejects unsupported types
            return expr;
        }
        return SqlFragment.concat(
                SqlFragment.of("COALESCE("),
                expr,
                SqlFragment.of(", " + SqlLiteral.typed(f.getDefaultValue(), f.getType()) + ")"));
    }
}
