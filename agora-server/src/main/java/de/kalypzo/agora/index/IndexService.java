package de.kalypzo.agora.index;

import de.kalypzo.agora.proto.v1.IndexDeclaration;
import de.kalypzo.agora.sql.FieldExpr;
import de.kalypzo.agora.sql.SqlLiteral;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Declared expression indexes (agora.md §5 Indizes, §7 Indizes). Each index is built once with
 * {@code CREATE INDEX CONCURRENTLY} on an autocommit connection; the state lives in {@code pantheon_index}
 * ({@code pending} -> {@code ready} | {@code failed}). A failed index is dropped and retried on the next declaration
 * (E21); an index that is {@code ready} is not touched. Concurrent declarations of one name share a future.
 *
 * <p>The index expression is {@link FieldExpr#read} with no alias, textually the same as in queries (R4); unique
 * indexes leave out the default (R43).
 */
public final class IndexService {

    private static final Logger LOG = LoggerFactory.getLogger(IndexService.class);
    private static final String UNIQUE_VIOLATION = "23505";

    private final DataSource ds;
    private final ExecutorService executor = Executors.newFixedThreadPool(4, r -> {
        Thread t = new Thread(r, "agora-index");
        t.setDaemon(true);
        return t;
    });
    private final Map<String, CompletableFuture<String>> inFlight = new HashMap<>();

    public IndexService(DataSource ds) {
        this.ds = ds;
    }

    /** Stops accepting builds; a running {@code CREATE INDEX} is interrupted. */
    public void close() {
        executor.shutdownNow();
    }

    /**
     * Declares the index and completes with its name. Invalid declarations throw {@link IllegalArgumentException};
     * a failed build completes exceptionally with {@link IndexDeclarationException}.
     */
    public synchronized CompletableFuture<String> declare(IndexDeclaration decl) {
        String name = IndexNames.of(decl);
        String definition = definition(name, decl);
        CompletableFuture<String> running = inFlight.get(name);
        if (running != null) return running;
        CompletableFuture<String> future = new CompletableFuture<>();
        inFlight.put(name, future);
        executor.execute(() -> {
            try {
                build(name, definition);
                finish(name, future, null);
            } catch (Throwable t) {
                finish(name, future, t);
            }
        });
        return future;
    }

    private void finish(String name, CompletableFuture<String> future, Throwable error) {
        synchronized (this) {
            inFlight.remove(name);
        }
        if (error == null) future.complete(name);
        else future.completeExceptionally(error);
    }

    /**
     * The index covers the ranking query {@code ORDER BY <expr> DESC NULLS LAST, <tie-break> ASC} (R39): the tie-break
     * is {@code id COLLATE "C"} for stores and {@code player} for players. ASC sorts are not index-backed. A unique
     * index covers the raw expression alone (a tie-break column would make every entry unique), without the default
     * and only where the field is present (R43). A changed definition rebuilds the index (R40).
     */
    static String definition(String name, IndexDeclaration decl) {
        // R43: a unique index covers only explicitly set values: the raw read without the default's COALESCE,
        // partial on "field present", so rows without the field never collide.
        String expr = decl.getUnique()
                ? FieldExpr.read("", decl.getField().toBuilder().clearDefaultValue().build()).sql()
                : FieldExpr.read("", decl.getField()).sql();
        String table;
        String tieBreak;
        String predicate;
        switch (decl.getTarget().getTargetCase()) {
            case PLAYERS -> {
                table = "pantheon_player";
                tieBreak = "player ASC";
                predicate = "namespace = " + SqlLiteral.namespace(decl.getField().getNamespace());
            }
            case STORE -> {
                table = "pantheon_doc";
                tieBreak = "id COLLATE \"C\" ASC";
                predicate = "store = " + SqlLiteral.store(decl.getTarget().getStore());
            }
            default -> throw new IllegalArgumentException("index has no target");
        }
        if (decl.getUnique()) {
            predicate += " AND (data->'" + SqlLiteral.fieldName(decl.getField().getName()) + "') IS NOT NULL";
        }
        String columns = decl.getUnique() ? "((" + expr + "))" : "((" + expr + ") DESC NULLS LAST, " + tieBreak + ")";
        return "CREATE " + (decl.getUnique() ? "UNIQUE " : "") + "INDEX CONCURRENTLY IF NOT EXISTS " + name + " ON "
                + table + " " + columns + " WHERE " + predicate;
    }

    private void build(String name, String definition) throws SQLException {
        try (Connection c = ds.getConnection()) {
            c.setAutoCommit(true); // CONCURRENTLY cannot run inside a transaction
            String[] current = state(c, name);
            if (current != null && "ready".equals(current[0]) && definition.equals(current[1])) return;
            // New, failed, interrupted while pending, or a changed definition (R40): (re)build from a clean
            // state, created_at stays.
            try (PreparedStatement ps = c.prepareStatement(
                    "INSERT INTO pantheon_index (name, definition, status) VALUES (?, ?, 'pending') "
                            + "ON CONFLICT (name) DO UPDATE SET definition = EXCLUDED.definition, status = 'pending', "
                            + "error = NULL")) {
                ps.setString(1, name);
                ps.setString(2, definition);
                ps.executeUpdate();
            }
            dropIndex(c, name);
            try (Statement s = c.createStatement()) {
                s.execute(definition);
                setStatus(c, name, "ready", null);
            } catch (SQLException e) {
                LOG.warn("Index {} failed: {}", name, e.getMessage());
                try {
                    dropIndex(c, name); // invalid remnant of the failed CONCURRENTLY build
                } catch (SQLException drop) {
                    LOG.error("Could not drop invalid index {}", name, drop);
                }
                String message = e.getMessage() == null ? e.getClass().getSimpleName() : e.getMessage();
                setStatus(c, name, "failed", message);
                throw new IndexDeclarationException(message, UNIQUE_VIOLATION.equals(e.getSQLState()));
            }
        }
    }

    /** {status, definition} of the declared index, or null. */
    private static String[] state(Connection c, String name) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("SELECT status, definition FROM pantheon_index WHERE name = ?")) {
            ps.setString(1, name);
            try (ResultSet rs = ps.executeQuery()) {
                return rs.next() ? new String[] {rs.getString(1), rs.getString(2)} : null;
            }
        }
    }

    private static void setStatus(Connection c, String name, String status, String error) throws SQLException {
        try (PreparedStatement ps = c.prepareStatement("UPDATE pantheon_index SET status = ?, error = ? WHERE name = ?")) {
            ps.setString(1, status);
            ps.setString(2, error);
            ps.setString(3, name);
            ps.executeUpdate();
        }
    }

    private static void dropIndex(Connection c, String name) throws SQLException {
        try (Statement s = c.createStatement()) {
            s.execute("DROP INDEX CONCURRENTLY IF EXISTS " + name);
        }
    }
}
