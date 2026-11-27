package de.kalypzo.agora.sql;

import de.kalypzo.agora.proto.v1.ScalarType;
import de.kalypzo.agora.proto.v1.Value;
import java.util.regex.Pattern;

/**
 * Validated names and injection-safe SQL literals (ruling R4).
 *
 * <p>Only data that the plugin declares (field names, namespaces, stores, defaults) is ever emitted as a literal, so
 * that expression indexes can repeat the exact expression. Values of operations and conditions are always bound.
 */
public final class SqlLiteral {

    private static final Pattern NAME = Pattern.compile("[a-z0-9_]+");
    private static final Pattern STORE = Pattern.compile("[a-z0-9_]+:[a-z0-9_]+");
    private static final Pattern IDENT = Pattern.compile("[a-z_][a-z0-9_]*");
    private static final Pattern COLUMN = Pattern.compile("[a-z_][a-z0-9_]*(\\.[a-z_][a-z0-9_]*)?");

    private SqlLiteral() {}

    /** Returns the field name if it matches {@code [a-z0-9_]+}, otherwise throws {@link IllegalArgumentException}. */
    public static String fieldName(String name) {
        return requireMatch(NAME, name, "field name");
    }

    /** The namespace as a quoted SQL literal, e.g. {@code 'economy'}. */
    public static String namespace(String namespace) {
        return "'" + requireMatch(NAME, namespace, "namespace") + "'";
    }

    /** The store ({@code <namespace>:<store>}) as a quoted SQL literal, e.g. {@code 'lobby:clans'}. */
    public static String store(String store) {
        return "'" + requireMatch(STORE, store, "store") + "'";
    }

    /**
     * An arbitrary string as a SQL string literal. Quotes are doubled; if the text contains a backslash it is written
     * as an escape string ({@code E'…'}) with doubled backslashes, so the result is the same whatever
     * {@code standard_conforming_strings} says. NUL characters are rejected (Postgres text cannot hold them).
     */
    public static String text(String s) {
        if (s.indexOf('\0') >= 0) throw new IllegalArgumentException("string contains NUL character");
        String quoted = s.replace("'", "''");
        if (s.indexOf('\\') >= 0) return "E'" + quoted.replace("\\", "\\\\") + "'";
        return "'" + quoted + "'";
    }

    /**
     * A default value as a typed SQL literal for a field of the given type, e.g. {@code '5'::bigint},
     * {@code '1.5'::double precision}, {@code true}, {@code 'abc'} or {@code '[]'::jsonb}.
     */
    public static String typed(Value v, ScalarType type) {
        Values.requireCompatible(v, type, "default");
        if (Values.isNull(v)) return "NULL::" + sqlType(type);
        if (v.hasDoubleValue() && !Double.isFinite(v.getDoubleValue())) {
            throw new IllegalArgumentException("default must be finite, was " + v.getDoubleValue());
        }
        return switch (type) {
            case LONG -> "'" + v.getLongValue() + "'::bigint";
            case DOUBLE -> "'" + (v.hasLongValue() ? Long.toString(v.getLongValue()) : Double.toString(v.getDoubleValue()))
                    + "'::double precision";
            case BOOLEAN -> v.getBoolValue() ? "true" : "false";
            case STRING -> text(v.getStringValue());
            case LIST, SET, OBJECT -> text(Values.toJson(v)) + "::jsonb";
            default -> throw new IllegalArgumentException("unsupported type " + type);
        };
    }

    /** The SQL type a field of this type is read as. */
    public static String sqlType(ScalarType type) {
        return switch (type) {
            case LONG -> "bigint";
            case DOUBLE -> "double precision";
            case BOOLEAN -> "boolean";
            case STRING -> "text";
            case LIST, SET, OBJECT -> "jsonb";
            default -> throw new IllegalArgumentException("unsupported type " + type);
        };
    }

    /** {@code <alias>.data}, or plain {@code data} for a null or empty alias (e.g. in index definitions). */
    static String dataColumn(String alias) {
        if (alias == null || alias.isEmpty()) return "data";
        return requireMatch(IDENT, alias, "alias") + ".data";
    }

    /** A (possibly qualified) column name such as {@code d.id}. */
    static String column(String column) {
        return requireMatch(COLUMN, column, "column");
    }

    private static String requireMatch(Pattern p, String s, String what) {
        if (s == null || !p.matcher(s).matches()) {
            throw new IllegalArgumentException("invalid " + what + ": " + s);
        }
        return s;
    }
}
