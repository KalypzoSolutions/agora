package de.kalypzo.agora.data;

import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * The idempotency table for every write RPC (agora.md §5, R22). Keys are kept 24 h; the cleanup job (A-13) deletes
 * older ones.
 *
 * <p>A key is claimed inside the write transaction with {@code INSERT … ON CONFLICT DO NOTHING}: a concurrent request
 * with the same key blocks on the unique index until the first one commits (then it reads the stored result) or rolls
 * back (then it claims the key itself). The result is stored as {@code {"rpc": "<call>", "response": <JsonFormat>}},
 * so a key reused for a different call is recognised.
 */
final class Idempotency {

    private static final int QUERY_TIMEOUT_SECONDS = 5;
    private static final JsonFormat.Printer PRINTER = JsonFormat.printer().omittingInsignificantWhitespace();
    private static final JsonFormat.Parser PARSER = JsonFormat.parser();

    private Idempotency() {}

    /** The stored result of a key that was seen before. */
    record Stored(String rpc, String responseJson) {}

    static String requireKey(String key) {
        if (key == null || key.isBlank()) throw new IllegalArgumentException("idempotency key missing");
        return key;
    }

    /** Claims the key in the current transaction; returns {@code null} if it is new, else its stored result. */
    static Stored claim(Connection c, String key) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement(
                "INSERT INTO pantheon_idempotency (idempotency_key, result) VALUES (?, 'null') ON CONFLICT DO NOTHING")) {
            ps.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
            ps.setString(1, key);
            if (ps.executeUpdate() == 1) return null;
        }
        try (PreparedStatement ps = c.prepareStatement(
                "SELECT result->>'rpc', (result->'response')::text FROM pantheon_idempotency WHERE idempotency_key = ?")) {
            ps.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
            ps.setString(1, key);
            try (ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) {
                    // deleted by the cleanup in between: treat like a transient failure, the client retries
                    throw new java.sql.SQLTransientException("idempotency key vanished", "40001");
                }
                return new Stored(rs.getString(1), rs.getString(2));
            }
        }
    }

    /** Stores the result of a claimed key (in the same transaction). */
    static void record(Connection c, String key, String rpc, Message response) throws SQLException {
        String json;
        try {
            json = "{\"rpc\":\"" + rpc + "\",\"response\":" + PRINTER.print(response) + "}";
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("response not printable as JSON", e);
        }
        try (PreparedStatement ps = c.prepareStatement(
                "UPDATE pantheon_idempotency SET result = ?::jsonb WHERE idempotency_key = ?")) {
            ps.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
            ps.setString(1, json);
            ps.setString(2, key);
            ps.executeUpdate();
        }
    }

    /** The stored response; a key that was used for another call is an {@link IllegalArgumentException}. */
    @SuppressWarnings("unchecked")
    static <M extends Message> M replay(Stored stored, String key, String rpc, Message.Builder builder) {
        if (!rpc.equals(stored.rpc()) || stored.responseJson() == null) {
            throw new IllegalArgumentException("idempotency key " + key + " was used for a different call");
        }
        try {
            PARSER.merge(stored.responseJson(), builder);
        } catch (InvalidProtocolBufferException e) {
            throw new IllegalStateException("stored result of " + key + " is not readable", e);
        }
        return (M) builder.build();
    }
}
