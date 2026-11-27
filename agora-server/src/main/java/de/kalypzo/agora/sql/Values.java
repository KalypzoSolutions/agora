package de.kalypzo.agora.sql;

import de.kalypzo.agora.proto.v1.ScalarType;
import de.kalypzo.agora.proto.v1.Value;
import java.util.Map;

/** Type checks and JSON rendering for proto {@link Value}s. */
final class Values {

    private Values() {}

    static boolean isNull(Value v) {
        return v.getKindCase() == Value.KindCase.NULL_VALUE;
    }

    static boolean isJsonType(ScalarType t) {
        return t == ScalarType.LIST || t == ScalarType.SET || t == ScalarType.OBJECT;
    }

    /** Throws {@link IllegalArgumentException} unless {@code v} is null or fits a field of type {@code t}. */
    static void requireCompatible(Value v, ScalarType t, String what) {
        Value.KindCase k = v.getKindCase();
        if (k == Value.KindCase.KIND_NOT_SET) throw new IllegalArgumentException(what + " has no value");
        if (k == Value.KindCase.NULL_VALUE) return;
        boolean ok = switch (t) {
            case LONG -> k == Value.KindCase.LONG_VALUE;
            case DOUBLE -> k == Value.KindCase.DOUBLE_VALUE || k == Value.KindCase.LONG_VALUE;
            case BOOLEAN -> k == Value.KindCase.BOOL_VALUE;
            case STRING -> k == Value.KindCase.STRING_VALUE;
            case LIST, SET -> k == Value.KindCase.LIST_VALUE;
            case OBJECT -> k == Value.KindCase.MAP_VALUE;
            default -> throw new IllegalArgumentException("unsupported type " + t);
        };
        if (!ok) throw new IllegalArgumentException(what + " of kind " + k + " does not fit type " + t);
    }

    /** The JDBC parameter for a non-null scalar value. */
    static Object scalarParam(Value v) {
        return switch (v.getKindCase()) {
            case LONG_VALUE -> v.getLongValue();
            case DOUBLE_VALUE -> v.getDoubleValue();
            case BOOL_VALUE -> v.getBoolValue();
            case STRING_VALUE -> v.getStringValue();
            default -> throw new IllegalArgumentException("not a scalar value: " + v.getKindCase());
        };
    }

    /** The value as JSON text. Longs stay exact; NaN, infinities and NUL characters are rejected. */
    static String toJson(Value v) {
        StringBuilder sb = new StringBuilder();
        write(sb, v);
        return sb.toString();
    }

    private static void write(StringBuilder sb, Value v) {
        switch (v.getKindCase()) {
            case NULL_VALUE -> sb.append("null");
            case BOOL_VALUE -> sb.append(v.getBoolValue());
            case LONG_VALUE -> sb.append(v.getLongValue());
            case DOUBLE_VALUE -> {
                double d = v.getDoubleValue();
                if (!Double.isFinite(d)) throw new IllegalArgumentException("not a JSON number: " + d);
                sb.append(Double.toString(d));
            }
            case STRING_VALUE -> string(sb, v.getStringValue());
            case LIST_VALUE -> {
                sb.append('[');
                for (int i = 0; i < v.getListValue().getValuesCount(); i++) {
                    if (i > 0) sb.append(',');
                    write(sb, v.getListValue().getValues(i));
                }
                sb.append(']');
            }
            case MAP_VALUE -> {
                sb.append('{');
                boolean first = true;
                for (Map.Entry<String, Value> e : v.getMapValue().getFieldsMap().entrySet()) {
                    if (!first) sb.append(',');
                    first = false;
                    string(sb, e.getKey());
                    sb.append(':');
                    write(sb, e.getValue());
                }
                sb.append('}');
            }
            case KIND_NOT_SET -> throw new IllegalArgumentException("value has no kind");
        }
    }

    private static void string(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '\0' -> throw new IllegalArgumentException("string contains NUL character");
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (c < 0x20) sb.append(String.format("\\u%04x", (int) c));
                    else sb.append(c);
                }
            }
        }
        sb.append('"');
    }
}
