package de.kalypzo.agora.data;

import com.google.protobuf.Message;
import de.kalypzo.agora.proto.v1.ApplyRequest;
import de.kalypzo.agora.proto.v1.ApplyResponse;
import de.kalypzo.agora.proto.v1.Condition;
import de.kalypzo.agora.proto.v1.CreateRequest;
import de.kalypzo.agora.proto.v1.CreateResponse;
import de.kalypzo.agora.proto.v1.DeleteRequest;
import de.kalypzo.agora.proto.v1.DeleteResponse;
import de.kalypzo.agora.proto.v1.DocChanged;
import de.kalypzo.agora.proto.v1.DocRef;
import de.kalypzo.agora.proto.v1.ExpireRequest;
import de.kalypzo.agora.proto.v1.ExpireResponse;
import de.kalypzo.agora.proto.v1.FieldOp;
import de.kalypzo.agora.proto.v1.FieldSpec;
import de.kalypzo.agora.proto.v1.PlayerRow;
import de.kalypzo.agora.proto.v1.Row;
import de.kalypzo.agora.proto.v1.RowRef;
import de.kalypzo.agora.proto.v1.ScalarType;
import de.kalypzo.agora.proto.v1.StoreDoc;
import de.kalypzo.agora.proto.v1.TransferRequest;
import de.kalypzo.agora.proto.v1.TransferResponse;
import de.kalypzo.agora.proto.v1.Value;
import de.kalypzo.agora.sql.PredicateSql;
import de.kalypzo.agora.sql.SqlFragment;
import de.kalypzo.agora.sql.SqlLiteral;
import de.kalypzo.agora.sql.UpdateBuilder;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTransientException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import javax.sql.DataSource;
import org.postgresql.util.PSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The write RPCs over Postgres (agora.md §5 "Bedingte und atomare Operationen", "Idempotenz", §7).
 *
 * <p>Every call runs in <b>one</b> transaction: claim the idempotency key (a replay returns the stored result), write,
 * store the result, commit, and only then hand every changed row to the {@link ChangePublisher} with the request's key
 * (E4). A failed condition rolls back and is <b>not</b> stored (Apply, Transfer); every other outcome is.
 *
 * <p>Rows are written in a fixed order (Apply: namespace ascending; Transfer: {@link RowRef} as string in UTF-8 byte
 * order, like {@code COLLATE "C"}), so writes
 * never deadlock. Expired store documents (not yet removed by the cleanup job) count as absent (R41): before any write
 * to a store its expired documents are deleted in the same transaction, published as {@code deleted} after the commit
 * (without a key, they are not the request's own change).
 *
 * <p>Errors: invalid requests (incl. the rejections of {@link UpdateBuilder#implicitConditions}) and Postgres data
 * exceptions (SQLSTATE class 22, e.g. 22003 bigint overflow of {@code inc}) are {@link IllegalArgumentException}s
 * ({@code INVALID_ARGUMENT}); a violated unique index (23505, not a primary key) is a
 * {@link UniqueViolationException} ({@code ALREADY_EXISTS}, description = index name).
 */
public final class WriteService {

    private static final Logger LOG = LoggerFactory.getLogger(WriteService.class);
    private static final int QUERY_TIMEOUT_SECONDS = 5;
    /** UTF-8 byte order, the same as Postgres {@code COLLATE "C"}: one lock order for Transfer and the purge (m-1). */
    private static final Comparator<String> BINARY = (x, y) -> Arrays.compareUnsigned(
            x.getBytes(StandardCharsets.UTF_8), y.getBytes(StandardCharsets.UTF_8));

    private final DataSource ds;
    private final ChangePublisher publisher;

    public WriteService(DataSource ds, ChangePublisher publisher) {
        this.ds = ds;
        this.publisher = publisher;
    }

    // ---------- Apply ----------

    public ApplyResponse apply(ApplyRequest request) throws SQLException {
        String key = Idempotency.requireKey(request.getIdempotencyKey());
        DocRef doc = request.getDoc();
        Map<String, List<FieldOp>> ops = new TreeMap<>();
        Map<String, List<Condition>> conditions = new TreeMap<>();
        for (FieldOp op : request.getOpsList()) {
            ops.computeIfAbsent(rowNamespace(doc, op.getField()), k -> new ArrayList<>()).add(op);
        }
        for (Condition c : request.getConditionsList()) {
            conditions.computeIfAbsent(rowNamespace(doc, c.getField()), k -> new ArrayList<>()).add(c);
        }
        switch (doc.getTargetCase()) {
            case PLAYER -> uuid(doc.getPlayer());
            case STORE_DOC -> validDoc(doc.getStoreDoc());
            case TARGET_NOT_SET -> throw new IllegalArgumentException("doc has no target");
        }
        List<RowWrite> rows = new ArrayList<>();
        TreeSet<String> namespaces = new TreeSet<>(ops.keySet());
        namespaces.addAll(conditions.keySet());
        for (String ns : namespaces) {
            List<FieldOp> rowOps = ops.getOrDefault(ns, List.of());
            List<Condition> rowConds = new ArrayList<>(conditions.getOrDefault(ns, List.of()));
            rowConds.addAll(UpdateBuilder.implicitConditions(rowOps));
            rows.add(RowWrite.of(rowRef(doc, ns), rowOps, rowConds));
        }
        List<String> stores = doc.hasStoreDoc() ? List.of(doc.getStoreDoc().getStore()) : List.of();

        return run("apply", key, ApplyResponse.newBuilder(), tx -> {
            tx.purgeExpired(stores);
            ApplyResponse.Builder out = ApplyResponse.newBuilder().setApplied(true);
            for (RowWrite row : rows) {
                Row written = tx.write(row);
                if (written == RowWrite.UNMET) return Outcome.rollback(ApplyResponse.newBuilder().setApplied(false).build());
                if (written != null) out.addRows(written);
            }
            return Outcome.commit(out.build());
        });
    }

    // ---------- Transfer ----------

    public TransferResponse transfer(TransferRequest request) throws SQLException {
        String key = Idempotency.requireKey(request.getIdempotencyKey());
        FieldSpec field = request.getField();
        if (request.getAmount() <= 0) {
            throw new IllegalArgumentException("transfer amount must be > 0, was " + request.getAmount());
        }
        if (field.getType() != ScalarType.LONG && field.getType() != ScalarType.DOUBLE) {
            throw new IllegalArgumentException("transfer needs a LONG or DOUBLE field: " + field.getName());
        }
        RowRef from = rowRef(request.getFrom(), rowNamespace(request.getFrom(), field));
        RowRef to = rowRef(request.getTo(), rowNamespace(request.getTo(), field));
        if (sortKey(from).equals(sortKey(to))) throw new IllegalArgumentException("transfer to the same row");
        FieldOp withdraw = FieldOp.newBuilder().setField(field).setWithdraw(request.getAmount()).build();
        FieldOp inc = FieldOp.newBuilder().setField(field).setInc(request.getAmount()).build();
        RowWrite fromWrite = RowWrite.of(from, List.of(withdraw), UpdateBuilder.implicitConditions(List.of(withdraw)));
        RowWrite toWrite = RowWrite.of(to, List.of(inc), List.of());
        List<RowWrite> ordered = new ArrayList<>(List.of(fromWrite, toWrite));
        ordered.sort(Comparator.comparing(w -> sortKey(w.ref()), BINARY));
        TreeSet<String> stores = new TreeSet<>(BINARY);
        for (RowRef r : List.of(from, to)) if (r.hasStoreDoc()) stores.add(r.getStoreDoc().getStore());

        return run("transfer", key, TransferResponse.newBuilder(), tx -> {
            tx.purgeExpired(stores);
            Map<String, Row> written = new TreeMap<>();
            for (RowWrite w : ordered) {
                Row row = tx.write(w);
                if (row == RowWrite.UNMET) {
                    return Outcome.rollback(TransferResponse.newBuilder().setTransferred(false).build());
                }
                written.put(sortKey(w.ref()), row);
            }
            return Outcome.commit(TransferResponse.newBuilder().setTransferred(true)
                    .addRows(written.get(sortKey(from)))
                    .addRows(written.get(sortKey(to)))
                    .build());
        });
    }

    // ---------- Create ----------

    public CreateResponse create(CreateRequest request) throws SQLException {
        String key = Idempotency.requireKey(request.getIdempotencyKey());
        StoreDoc doc = validDoc(request.getDoc());
        String namespace = storeNamespace(doc.getStore());
        List<FieldOp> sets = new ArrayList<>();
        for (Map.Entry<String, Value> e : new TreeMap<>(request.getDataMap()).entrySet()) {
            Value v = e.getValue();
            if (v.getKindCase() == Value.KindCase.NULL_VALUE) continue; // set(null) ≡ unset
            FieldSpec f = FieldSpec.newBuilder().setNamespace(namespace).setName(SqlLiteral.fieldName(e.getKey()))
                    .setType(typeOf(v)).build();
            sets.add(FieldOp.newBuilder().setField(f).setSet(v).build());
        }
        SqlFragment data = UpdateBuilder.dataExpression(sets);
        SqlFragment insert = SqlFragment.concat(
                SqlFragment.of("INSERT INTO pantheon_doc (store, id, data) SELECT ?, ", doc.getStore()),
                SqlFragment.of("?, ", doc.getId()),
                data,
                SqlFragment.of(" FROM (SELECT '{}'::jsonb AS data) AS s ON CONFLICT (store, id) DO NOTHING"
                        + " RETURNING data::text, version"));

        return run("create", key, CreateResponse.newBuilder(), tx -> {
            tx.purgeExpired(List.of(doc.getStore()));
            try (PreparedStatement ps = tx.prepare(insert); ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Outcome.commit(CreateResponse.newBuilder().setCreated(false).build());
                Row row = tx.changed(storeRef(doc), rs.getString(1), rs.getLong(2));
                return Outcome.commit(CreateResponse.newBuilder().setCreated(true).setRow(row).build());
            }
        });
    }

    // ---------- Delete ----------

    public DeleteResponse delete(DeleteRequest request) throws SQLException {
        String key = Idempotency.requireKey(request.getIdempotencyKey());
        StoreDoc doc = validDoc(request.getDoc());
        return run("delete", key, DeleteResponse.newBuilder(), tx -> {
            tx.purgeExpired(List.of(doc.getStore()));
            try (PreparedStatement ps = tx.prepare(SqlFragment.concat(
                            SqlFragment.of("DELETE FROM pantheon_doc WHERE store = ? ", doc.getStore()),
                            SqlFragment.of("AND id = ? RETURNING nextval('pantheon_version')", doc.getId())));
                    ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return Outcome.commit(DeleteResponse.newBuilder().setDeleted(false).build());
                tx.deleted(storeRef(doc), rs.getLong(1), key);
                return Outcome.commit(DeleteResponse.newBuilder().setDeleted(true).build());
            }
        });
    }

    // ---------- Expire ----------

    /** Sets {@code expires_at}; the document keeps its version and no change is published for it. */
    public ExpireResponse expire(ExpireRequest request) throws SQLException {
        String key = Idempotency.requireKey(request.getIdempotencyKey());
        StoreDoc doc = validDoc(request.getDoc());
        OffsetDateTime at = OffsetDateTime.ofInstant(Instant.ofEpochMilli(request.getAtMillis()), ZoneOffset.UTC);
        return run("expire", key, ExpireResponse.newBuilder(), tx -> {
            tx.purgeExpired(List.of(doc.getStore()));
            try (PreparedStatement ps = tx.prepare(SqlFragment.concat(
                    SqlFragment.of("UPDATE pantheon_doc SET expires_at = ? ", at),
                    SqlFragment.of("WHERE store = ? ", doc.getStore()),
                    SqlFragment.of("AND id = ?", doc.getId())))) {
                ps.executeUpdate();
            }
            return Outcome.commit(ExpireResponse.getDefaultInstance());
        });
    }

    // ---------- transaction ----------

    private record Outcome<R>(R response, boolean commit) {
        static <R> Outcome<R> commit(R response) {
            return new Outcome<>(response, true);
        }

        static <R> Outcome<R> rollback(R response) {
            return new Outcome<>(response, false);
        }
    }

    @FunctionalInterface
    private interface Body<R> {
        Outcome<R> run(Tx tx) throws SQLException;
    }

    /** Claims the key, runs the body, stores the result, commits, then publishes. */
    private <R extends Message> R run(String rpc, String key, Message.Builder replayBuilder, Body<R> body)
            throws SQLException {
        List<DocChanged> changes = new ArrayList<>();
        R response;
        try (Connection c = ds.getConnection()) {
            c.setAutoCommit(false);
            try {
                Idempotency.Stored stored = Idempotency.claim(c, key);
                if (stored != null) {
                    c.rollback();
                    return Idempotency.replay(stored, key, rpc, replayBuilder);
                }
                Tx tx = new Tx(c, key, changes);
                Outcome<R> outcome = body.run(tx);
                if (!outcome.commit()) {
                    c.rollback();
                    return outcome.response();
                }
                Idempotency.record(c, key, rpc, (Message) outcome.response());
                c.commit();
                response = outcome.response();
            } catch (SQLException e) {
                rollbackQuietly(c);
                throw translate(e);
            } catch (RuntimeException e) {
                rollbackQuietly(c);
                throw e;
            } finally {
                try {
                    c.setAutoCommit(true);
                } catch (SQLException e) {
                    // after a commit the result stands; the pool discards the broken connection
                    LOG.warn("Resetting autocommit failed", e);
                }
            }
        }
        for (DocChanged change : changes) {
            try {
                publisher.publish(change);
            } catch (RuntimeException e) {
                LOG.error("Publishing a change failed", e);
            }
        }
        return response;
    }

    private static void rollbackQuietly(Connection c) {
        try {
            c.rollback();
        } catch (SQLException e) {
            LOG.warn("Rollback failed", e);
        }
    }

    /** Unique violations, data exceptions and serialization failures get their own meaning; the rest stays. */
    private static SQLException translate(SQLException e) {
        String state = e.getSQLState();
        if ("23505".equals(state)) {
            String constraint = e instanceof PSQLException p && p.getServerErrorMessage() != null
                    ? p.getServerErrorMessage().getConstraint()
                    : null;
            if (constraint != null && !constraint.endsWith("_pkey")) throw new UniqueViolationException(constraint);
        }
        if (state != null && state.startsWith("22")) {
            throw new IllegalArgumentException("value out of range or invalid for the field: " + serverMessage(e), e);
        }
        if ("40001".equals(state) || "40P01".equals(state)) {
            return new SQLTransientException(e.getMessage(), state, e);
        }
        return e;
    }

    private static String serverMessage(SQLException e) {
        if (e instanceof PSQLException p && p.getServerErrorMessage() != null) {
            return p.getServerErrorMessage().getMessage();
        }
        return e.getMessage();
    }

    /** One open write transaction: statements and the changes to publish after the commit. */
    private static final class Tx {
        private final Connection c;
        private final String key;
        private final List<DocChanged> changes;

        Tx(Connection c, String key, List<DocChanged> changes) {
            this.c = c;
            this.key = key;
            this.changes = changes;
        }

        PreparedStatement prepare(SqlFragment sql) throws SQLException {
            PreparedStatement ps = c.prepareStatement(sql.sql());
            ps.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
            for (int i = 0; i < sql.params().size(); i++) ps.setObject(i + 1, sql.params().get(i));
            return ps;
        }

        /** R41: deletes the expired documents of these stores (in a fixed order) and publishes them as deleted. */
        void purgeExpired(Iterable<String> stores) throws SQLException {
            for (String store : stores) {
                try (PreparedStatement ps = prepare(SqlFragment.of(
                                "DELETE FROM pantheon_doc d USING (SELECT store, id FROM pantheon_doc WHERE store = ?"
                                        + " AND expires_at <= now() ORDER BY id COLLATE \"C\" FOR UPDATE) e"
                                        + " WHERE d.store = e.store AND d.id = e.id"
                                        + " RETURNING d.id, nextval('pantheon_version')",
                                store));
                        ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        RowRef ref = storeRef(StoreDoc.newBuilder().setStore(store).setId(rs.getString(1)).build());
                        deleted(ref, rs.getLong(2), "");
                    }
                }
            }
        }

        /**
         * Writes one row: inserts it with its computed data if missing (and its conditions hold for an empty row),
         * otherwise updates it under its conditions. A row with conditions only is
         * checked under a row lock (a missing row counts as empty, its defaults apply) and not created. Returns the
         * new row, {@code null} for a checked row, or {@link RowWrite#UNMET} if a condition failed.
         */
        Row write(RowWrite w) throws SQLException {
            if (w.ops().isEmpty()) {
                if (w.conditions().isEmpty()) return null;
                SqlFragment check = SqlFragment.concat(
                        SqlFragment.of("SELECT 1 FROM (SELECT COALESCE((SELECT x.data FROM " + w.table() + " x WHERE "),
                        w.key("x"),
                        SqlFragment.of(" FOR UPDATE), '{}'::jsonb) AS data) AS t WHERE "),
                        w.where());
                try (PreparedStatement ps = prepare(check); ResultSet rs = ps.executeQuery()) {
                    return rs.next() ? null : RowWrite.UNMET;
                }
            }
            // A missing row is inserted with its computed data in one statement (R43). The conflict target is the
            // primary key only, so a violated unique index raises 23505 (ALREADY_EXISTS) instead of being swallowed.
            // If the conditions do not hold for an empty row, a missing row is never created (the UPDATE below then
            // finds no row: UNMET).
            if (w.conditions().isEmpty() || holdsOnEmptyRow(w)) {
                try (PreparedStatement ps = prepare(w.insert()); ResultSet rs = ps.executeQuery()) {
                    if (rs.next()) return changed(w.ref(), rs.getString(1), rs.getLong(2));
                }
            }
            SqlFragment update = SqlFragment.concat(
                    SqlFragment.of("UPDATE " + w.table() + " AS t SET data = "),
                    w.data(),
                    SqlFragment.of(", version = nextval('pantheon_version'), updated_at = now() WHERE "),
                    w.key("t"),
                    w.conditions().isEmpty() ? SqlFragment.of("") : SqlFragment.concat(SqlFragment.of(" AND "), w.where()),
                    SqlFragment.of(" RETURNING t.data::text, t.version"));
            try (PreparedStatement ps = prepare(update); ResultSet rs = ps.executeQuery()) {
                if (!rs.next()) return RowWrite.UNMET;
                return changed(w.ref(), rs.getString(1), rs.getLong(2));
            }
        }

        /** Whether the row's conditions hold for a row that does not exist yet (empty data, defaults apply). */
        private boolean holdsOnEmptyRow(RowWrite w) throws SQLException {
            SqlFragment check = SqlFragment.concat(
                    SqlFragment.of("SELECT 1 FROM (SELECT '{}'::jsonb AS data) AS t WHERE "), w.where());
            try (PreparedStatement ps = prepare(check); ResultSet rs = ps.executeQuery()) {
                return rs.next();
            }
        }

        Row changed(RowRef ref, String json, long version) {
            Map<String, Value> data = JsonValues.parseObject(json);
            changes.add(DocChanged.newBuilder().setRef(ref).setVersion(version).putAllData(data)
                    .setIdempotencyKey(key).build());
            return Row.newBuilder().setRef(ref).setVersion(version).putAllData(data).build();
        }

        void deleted(RowRef ref, long version, String changeKey) {
            changes.add(DocChanged.newBuilder().setRef(ref).setVersion(version).setDeleted(true)
                    .setIdempotencyKey(changeKey).build());
        }
    }

    /** The prepared SQL parts for one row. */
    private record RowWrite(RowRef ref, List<FieldOp> ops, List<Condition> conditions, SqlFragment data, SqlFragment where) {

        /** Marker: a condition of this row is not met. */
        static final Row UNMET = Row.getDefaultInstance();

        /** Builds (and thereby validates) the SQL before the transaction starts. */
        static RowWrite of(RowRef ref, List<FieldOp> ops, List<Condition> conditions) {
            SqlFragment data = ops.isEmpty() ? null : UpdateBuilder.dataExpression(ops);
            List<SqlFragment> parts = new ArrayList<>();
            for (Condition c : conditions) parts.add(PredicateSql.where("t", c));
            return new RowWrite(ref, ops, conditions, data, SqlFragment.join(" AND ", parts));
        }

        String table() {
            return ref.hasPlayerRow() ? "pantheon_player" : "pantheon_doc";
        }

        SqlFragment key(String alias) {
            if (ref.hasPlayerRow()) {
                return SqlFragment.concat(
                        SqlFragment.of(alias + ".player = ? ", UUID.fromString(ref.getPlayerRow().getPlayer())),
                        SqlFragment.of("AND " + alias + ".namespace = ?", ref.getPlayerRow().getNamespace()));
            }
            return SqlFragment.concat(
                    SqlFragment.of(alias + ".store = ? ", ref.getStoreDoc().getStore()),
                    SqlFragment.of("AND " + alias + ".id = ?", ref.getStoreDoc().getId()));
        }

        /**
         * {@code INSERT … SELECT <key>, <data computed over '{}'> … ON CONFLICT (<primary key>) DO NOTHING
         * RETURNING data::text, version}: no row back means the row already exists.
         */
        SqlFragment insert() {
            SqlFragment head;
            String target;
            if (ref.hasPlayerRow()) {
                head = SqlFragment.concat(
                        SqlFragment.of("INSERT INTO pantheon_player (player, namespace, data) SELECT ?, ",
                                UUID.fromString(ref.getPlayerRow().getPlayer())),
                        SqlFragment.of("?, ", ref.getPlayerRow().getNamespace()));
                target = "(player, namespace)";
            } else {
                head = SqlFragment.concat(
                        SqlFragment.of("INSERT INTO pantheon_doc (store, id, data) SELECT ?, ", ref.getStoreDoc().getStore()),
                        SqlFragment.of("?, ", ref.getStoreDoc().getId()));
                target = "(store, id)";
            }
            return SqlFragment.concat(
                    head,
                    data,
                    SqlFragment.of(" FROM (SELECT '{}'::jsonb AS data) AS s ON CONFLICT " + target
                            + " DO NOTHING RETURNING data::text, version"));
        }
    }

    // ---------- validation, refs ----------

    /** The namespace of the row a field lives in: the field's for a player, the store's for a document. */
    private static String rowNamespace(DocRef doc, FieldSpec field) {
        SqlLiteral.namespace(field.getNamespace());
        SqlLiteral.fieldName(field.getName());
        switch (doc.getTargetCase()) {
            case PLAYER -> uuid(doc.getPlayer());
            case STORE_DOC -> {
                StoreDoc d = validDoc(doc.getStoreDoc());
                String ns = storeNamespace(d.getStore());
                if (!ns.equals(field.getNamespace())) {
                    throw new IllegalArgumentException("field " + field.getNamespace() + "." + field.getName()
                            + " does not belong to store " + d.getStore());
                }
            }
            case TARGET_NOT_SET -> throw new IllegalArgumentException("doc has no target");
        }
        return field.getNamespace();
    }

    private static RowRef rowRef(DocRef doc, String namespace) {
        return switch (doc.getTargetCase()) {
            case PLAYER -> RowRef.newBuilder().setPlayerRow(PlayerRow.newBuilder()
                    .setPlayer(uuid(doc.getPlayer()).toString()).setNamespace(namespace)).build();
            case STORE_DOC -> storeRef(doc.getStoreDoc());
            case TARGET_NOT_SET -> throw new IllegalArgumentException("doc has no target");
        };
    }

    private static RowRef storeRef(StoreDoc doc) {
        return RowRef.newBuilder().setStoreDoc(doc).build();
    }

    /** The lock order of Transfer. */
    private static String sortKey(RowRef ref) {
        return ref.hasPlayerRow()
                ? "player:" + ref.getPlayerRow().getPlayer() + "/" + ref.getPlayerRow().getNamespace()
                : "doc:" + ref.getStoreDoc().getStore() + "/" + ref.getStoreDoc().getId();
    }

    private static StoreDoc validDoc(StoreDoc doc) {
        SqlLiteral.store(doc.getStore());
        String id = doc.getId();
        if (id.isEmpty() || id.indexOf('\0') >= 0) throw new IllegalArgumentException("invalid document id: " + id);
        return doc;
    }

    private static String storeNamespace(String store) {
        SqlLiteral.store(store);
        return store.substring(0, store.indexOf(':'));
    }

    private static ScalarType typeOf(Value v) {
        return switch (v.getKindCase()) {
            case BOOL_VALUE -> ScalarType.BOOLEAN;
            case LONG_VALUE -> ScalarType.LONG;
            case DOUBLE_VALUE -> ScalarType.DOUBLE;
            case STRING_VALUE -> ScalarType.STRING;
            case LIST_VALUE -> ScalarType.LIST;
            case MAP_VALUE -> ScalarType.OBJECT;
            case NULL_VALUE, KIND_NOT_SET -> throw new IllegalArgumentException("create value has no kind");
        };
    }

    private static UUID uuid(String s) {
        try {
            return UUID.fromString(s);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("invalid player UUID: " + s);
        }
    }
}
