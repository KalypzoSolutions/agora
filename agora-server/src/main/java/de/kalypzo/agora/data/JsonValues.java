package de.kalypzo.agora.data;

import de.kalypzo.agora.proto.v1.Value;
import de.kalypzo.agora.proto.v1.ValueList;
import de.kalypzo.agora.proto.v1.ValueMap;
import java.util.Map;

/** Parses the JSON text of a {@code jsonb} column into proto {@link Value}s. Integers that fit a long stay exact. */
final class JsonValues {

    private final String s;
    private int pos;

    private JsonValues(String s) {
        this.s = s;
    }

    /** Parses a JSON object into its fields. */
    static Map<String, Value> parseObject(String json) {
        Value v = new JsonValues(json).parseTop();
        if (v.getKindCase() != Value.KindCase.MAP_VALUE) throw new IllegalArgumentException("not a JSON object");
        return v.getMapValue().getFieldsMap();
    }

    private Value parseTop() {
        Value v = value();
        ws();
        if (pos != s.length()) throw error("trailing data");
        return v;
    }

    private Value value() {
        ws();
        if (pos >= s.length()) throw error("unexpected end");
        char c = s.charAt(pos);
        return switch (c) {
            case '{' -> object();
            case '[' -> array();
            case '"' -> Value.newBuilder().setStringValue(string()).build();
            case 't' -> literal("true", Value.newBuilder().setBoolValue(true).build());
            case 'f' -> literal("false", Value.newBuilder().setBoolValue(false).build());
            case 'n' -> literal("null", Value.newBuilder().setNullValue(true).build());
            default -> number();
        };
    }

    private Value literal(String word, Value v) {
        if (!s.startsWith(word, pos)) throw error("bad literal");
        pos += word.length();
        return v;
    }

    private Value object() {
        ValueMap.Builder map = ValueMap.newBuilder();
        pos++;
        ws();
        if (peek() == '}') {
            pos++;
        } else {
            while (true) {
                ws();
                String key = string();
                ws();
                expect(':');
                map.putFields(key, value());
                ws();
                if (peek() == ',') {
                    pos++;
                } else {
                    expect('}');
                    break;
                }
            }
        }
        return Value.newBuilder().setMapValue(map).build();
    }

    private Value array() {
        ValueList.Builder list = ValueList.newBuilder();
        pos++;
        ws();
        if (peek() == ']') {
            pos++;
        } else {
            while (true) {
                list.addValues(value());
                ws();
                if (peek() == ',') {
                    pos++;
                } else {
                    expect(']');
                    break;
                }
            }
        }
        return Value.newBuilder().setListValue(list).build();
    }

    private String string() {
        expect('"');
        StringBuilder sb = new StringBuilder();
        while (true) {
            if (pos >= s.length()) throw error("unterminated string");
            char c = s.charAt(pos++);
            if (c == '"') return sb.toString();
            if (c != '\\') {
                sb.append(c);
                continue;
            }
            if (pos >= s.length()) throw error("unterminated escape");
            char e = s.charAt(pos++);
            switch (e) {
                case '"', '\\', '/' -> sb.append(e);
                case 'b' -> sb.append('\b');
                case 'f' -> sb.append('\f');
                case 'n' -> sb.append('\n');
                case 'r' -> sb.append('\r');
                case 't' -> sb.append('\t');
                case 'u' -> {
                    if (pos + 4 > s.length()) throw error("bad unicode escape");
                    sb.append((char) Integer.parseInt(s.substring(pos, pos + 4), 16));
                    pos += 4;
                }
                default -> throw error("bad escape");
            }
        }
    }

    private Value number() {
        int start = pos;
        boolean integral = true;
        while (pos < s.length()) {
            char c = s.charAt(pos);
            if (c == '.' || c == 'e' || c == 'E') integral = false;
            else if (!(c == '-' || c == '+' || (c >= '0' && c <= '9'))) break;
            pos++;
        }
        String text = s.substring(start, pos);
        if (text.isEmpty()) throw error("unexpected character");
        if (integral) {
            try {
                return Value.newBuilder().setLongValue(Long.parseLong(text)).build();
            } catch (NumberFormatException overflow) {
                // falls through to double
            }
        }
        try {
            return Value.newBuilder().setDoubleValue(Double.parseDouble(text)).build();
        } catch (NumberFormatException e) {
            throw error("bad number");
        }
    }

    private char peek() {
        return pos < s.length() ? s.charAt(pos) : '\0';
    }

    private void expect(char c) {
        if (peek() != c) throw error("expected '" + c + "'");
        pos++;
    }

    private void ws() {
        while (pos < s.length() && Character.isWhitespace(s.charAt(pos))) pos++;
    }

    private IllegalArgumentException error(String what) {
        return new IllegalArgumentException("invalid JSON at " + pos + ": " + what);
    }
}
