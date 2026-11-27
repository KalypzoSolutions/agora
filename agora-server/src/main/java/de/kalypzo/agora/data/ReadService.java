package de.kalypzo.agora.data;

import de.kalypzo.agora.proto.v1.Condition;
import de.kalypzo.agora.proto.v1.CountRequest;
import de.kalypzo.agora.proto.v1.CountResponse;
import de.kalypzo.agora.proto.v1.DocRef;
import de.kalypzo.agora.proto.v1.DocSnapshot;
import de.kalypzo.agora.proto.v1.FieldSpec;
import de.kalypzo.agora.proto.v1.LoadPlayerResponse;
import de.kalypzo.agora.proto.v1.LoadResponse;
import de.kalypzo.agora.proto.v1.PlayerRow;
import de.kalypzo.agora.proto.v1.QueryResponse;
import de.kalypzo.agora.proto.v1.QuerySpec;
import de.kalypzo.agora.proto.v1.QueryTarget;
import de.kalypzo.agora.proto.v1.Row;
import de.kalypzo.agora.proto.v1.RowRef;
import de.kalypzo.agora.proto.v1.RowVersion;
import de.kalypzo.agora.proto.v1.Sort;
import de.kalypzo.agora.proto.v1.StoreDoc;
import de.kalypzo.agora.proto.v1.VersionsRequest;
import de.kalypzo.agora.proto.v1.VersionsResponse;
import de.kalypzo.agora.sql.PredicateSql;
import de.kalypzo.agora.sql.SortSql;
import de.kalypzo.agora.sql.SqlFragment;
import de.kalypzo.agora.sql.SqlLiteral;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Function;
import javax.sql.DataSource;

/**
 * The read RPCs over Postgres (agora.md §5, §7; E6, E7, E9). Invalid requests throw
 * {@link IllegalArgumentException}; the endpoints turn that into {@code INVALID_ARGUMENT}.
 */
public final class ReadService {

    public static final int DEFAULT_LIMIT = 100;
    public static final int MAX_LIMIT = 1000;
    private static final int QUERY_TIMEOUT_SECONDS = 5;
    private static final String PANTHEON = "pantheon";
    private static final String NOT_EXPIRED = "(d.expires_at IS NULL OR d.expires_at > now())";

    private final DataSource ds;
    private final HolderSink holders;

    public ReadService(DataSource ds, HolderSink holders) {
        this.ds = ds;
        this.holders = holders;
    }

    // ---------- LoadPlayer / Load ----------

    /** Registers {@code nodeId} as holder first, then reads, so that no change falls between the two. */
    public LoadPlayerResponse loadPlayer(String nodeId, String player) throws SQLException {
        UUID id = uuid(player);
        holders.addPlayerHolder(nodeId, id);
        try (Connection c = ds.getConnection()) {
            return LoadPlayerResponse.newBuilder().addAllRows(playerRows(c, List.of(id)).getOrDefault(id, List.of())).build();
        }
    }

    public LoadResponse load(DocRef doc) throws SQLException {
        LoadResponse.Builder out = LoadResponse.newBuilder();
        try (Connection c = ds.getConnection()) {
            switch (doc.getTargetCase()) {
                case PLAYER -> {
                    UUID id = uuid(doc.getPlayer());
                    out.addAllRows(playerRows(c, List.of(id)).getOrDefault(id, List.of()));
                }
                case STORE_DOC -> {
                    StoreDoc sd = doc.getStoreDoc();
                    SqlLiteral.store(sd.getStore());
                    try (PreparedStatement ps = prepare(c,
                            "SELECT d.id, d.data::text, d.version FROM pantheon_doc d WHERE d.store = ? AND d.id = ? AND "
                                    + NOT_EXPIRED,
                            List.of(sd.getStore(), sd.getId()));
                            ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) out.addRows(docRow(sd.getStore(), rs));
                    }
                }
                case TARGET_NOT_SET -> throw new IllegalArgumentException("doc has no target");
            }
        }
        return out.build();
    }

    // ---------- Query / Count ----------

    public QueryResponse query(QueryTarget target, QuerySpec spec) throws SQLException {
        int limit = spec.getLimit() == 0 ? DEFAULT_LIMIT : spec.getLimit();
        if (limit < 1 || limit > MAX_LIMIT) {
            throw new IllegalArgumentException("limit must be 1.." + MAX_LIMIT + " (0 = " + DEFAULT_LIMIT + ")");
        }
        if (spec.getOffset() < 0) throw new IllegalArgumentException("offset must not be negative");
        QueryResponse.Builder out = QueryResponse.newBuilder();
        try (Connection c = ds.getConnection()) {
            switch (target.getTargetCase()) {
                case PLAYERS -> {
                    PlayerFrom from = PlayerFrom.of(spec.getWhereList(), spec.getSortList());
                    SqlFragment order = SortSql.orderBy(from::aliasFor, spec.getSortList(), "p0.player");
                    SqlFragment sql = SqlFragment.concat(
                            SqlFragment.of("SELECT p0.player "),
                            from.fromWhere(),
                            SqlFragment.of(" "),
                            order,
                            SqlFragment.of(" LIMIT " + limit + " OFFSET " + spec.getOffset()));
                    List<UUID> hits = new ArrayList<>();
                    try (PreparedStatement ps = prepare(c, sql.sql(), sql.params()); ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) hits.add(rs.getObject(1, UUID.class));
                    }
                    Map<UUID, List<Row>> rows = playerRows(c, hits);
                    for (UUID id : hits) {
                        out.addDocs(DocSnapshot.newBuilder()
                                .setDoc(DocRef.newBuilder().setPlayer(id.toString()))
                                .addAllRows(rows.getOrDefault(id, List.of())));
                    }
                }
                case STORE -> {
                    String store = target.getStore();
                    SqlFragment where = storeWhere(store, spec.getWhereList());
                    SqlFragment order = SortSql.orderBy(f -> "d", spec.getSortList(), "d.id", true);
                    SqlFragment sql = SqlFragment.concat(
                            SqlFragment.of("SELECT d.id, d.data::text, d.version FROM pantheon_doc d WHERE "),
                            where,
                            SqlFragment.of(" "),
                            order,
                            SqlFragment.of(" LIMIT " + limit + " OFFSET " + spec.getOffset()));
                    try (PreparedStatement ps = prepare(c, sql.sql(), sql.params()); ResultSet rs = ps.executeQuery()) {
                        while (rs.next()) {
                            Row row = docRow(store, rs);
                            out.addDocs(DocSnapshot.newBuilder()
                                    .setDoc(DocRef.newBuilder().setStoreDoc(row.getRef().getStoreDoc()))
                                    .addRows(row));
                        }
                    }
                }
                case TARGET_NOT_SET -> throw new IllegalArgumentException("query has no target");
            }
        }
        return out.build();
    }

    public CountResponse count(CountRequest request) throws SQLException {
        QueryTarget target = request.getTarget();
        try (Connection c = ds.getConnection()) {
            switch (target.getTargetCase()) {
                case PLAYERS -> {
                    PlayerFrom from = PlayerFrom.of(request.getWhereList(), List.of());
                    SqlFragment sql = SqlFragment.concat(SqlFragment.of("SELECT count(*) "), from.fromWhere());
                    try (PreparedStatement ps = prepare(c, sql.sql(), sql.params());
                            ResultSet rs = ps.executeQuery()) {
                        rs.next();
                        return CountResponse.newBuilder().setCount(rs.getLong(1)).build();
                    }
                }
                case STORE -> {
                    SqlFragment where = storeWhere(target.getStore(), request.getWhereList());
                    try (PreparedStatement ps = prepare(
                                    c, "SELECT count(*) FROM pantheon_doc d WHERE " + where.sql(), where.params());
                            ResultSet rs = ps.executeQuery()) {
                        rs.next();
                        return CountResponse.newBuilder().setCount(rs.getLong(1)).build();
                    }
                }
                default -> throw new IllegalArgumentException("count has no target");
            }
        }
    }

    // ---------- Versions ----------

    /** One version per requested row (missing or expired: 0), then every row of each requested player. */
    public VersionsResponse versions(VersionsRequest request) throws SQLException {
        List<UUID> playerIds = new ArrayList<>();
        List<String> playerNs = new ArrayList<>();
        List<String> stores = new ArrayList<>();
        List<String> docIds = new ArrayList<>();
        for (RowRef ref : request.getRowsList()) {
            switch (ref.getTargetCase()) {
                case PLAYER_ROW -> {
                    playerIds.add(uuid(ref.getPlayerRow().getPlayer()));
                    playerNs.add(SqlLiteral.fieldName(ref.getPlayerRow().getNamespace()));
                }
                case STORE_DOC -> {
                    stores.add(ref.getStoreDoc().getStore());
                    SqlLiteral.store(ref.getStoreDoc().getStore());
                    docIds.add(ref.getStoreDoc().getId());
                }
                case TARGET_NOT_SET -> throw new IllegalArgumentException("row ref has no target");
            }
        }
        List<UUID> players = new ArrayList<>();
        for (String p : request.getPlayersList()) players.add(uuid(p));
        List<String> rowKeys = new ArrayList<>();
        for (RowRef ref : request.getRowsList()) {
            rowKeys.add(ref.hasPlayerRow()
                    ? uuid(ref.getPlayerRow().getPlayer()) + "/" + ref.getPlayerRow().getNamespace()
                    : ref.getStoreDoc().getStore() + "/" + ref.getStoreDoc().getId());
        }

        VersionsResponse.Builder out = VersionsResponse.newBuilder();
        try (Connection c = ds.getConnection()) {
            Map<String, Long> playerVersions = new LinkedHashMap<>();
            if (!playerIds.isEmpty()) {
                Array ids = c.createArrayOf("uuid", playerIds.toArray());
                Array ns = c.createArrayOf("text", playerNs.toArray());
                try (PreparedStatement ps = prepare(c,
                                "SELECT p.player, p.namespace, p.version FROM pantheon_player p JOIN unnest(?::uuid[], ?::text[])"
                                        + " AS r(player, namespace) ON p.player = r.player AND p.namespace = r.namespace",
                                List.of(ids, ns));
                        ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        playerVersions.put(rs.getObject(1, UUID.class) + "/" + rs.getString(2), rs.getLong(3));
                    }
                }
            }
            Map<String, Long> docVersions = new LinkedHashMap<>();
            if (!stores.isEmpty()) {
                Array st = c.createArrayOf("text", stores.toArray());
                Array ids = c.createArrayOf("text", docIds.toArray());
                try (PreparedStatement ps = prepare(c,
                                "SELECT d.store, d.id, d.version FROM pantheon_doc d JOIN unnest(?::text[], ?::text[])"
                                        + " AS r(store, id) ON d.store = r.store AND d.id = r.id WHERE " + NOT_EXPIRED,
                                List.of(st, ids));
                        ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) docVersions.put(rs.getString(1) + "/" + rs.getString(2), rs.getLong(3));
                }
            }
            for (int i = 0; i < request.getRowsCount(); i++) {
                RowRef ref = request.getRows(i);
                long v = (ref.hasPlayerRow() ? playerVersions : docVersions).getOrDefault(rowKeys.get(i), 0L);
                out.addVersions(RowVersion.newBuilder().setRef(ref).setVersion(v));
            }
            if (!players.isEmpty()) {
                Array ids = c.createArrayOf("uuid", players.toArray());
                try (PreparedStatement ps = prepare(c,
                                "SELECT player, namespace, version FROM pantheon_player WHERE player = ANY(?)"
                                        + " ORDER BY player, namespace",
                                List.of(ids));
                        ResultSet rs = ps.executeQuery()) {
                    while (rs.next()) {
                        out.addVersions(RowVersion.newBuilder()
                                .setRef(playerRef(rs.getObject(1, UUID.class), rs.getString(2)))
                                .setVersion(rs.getLong(3)));
                    }
                }
            }
        }
        return out.build();
    }

    // ---------- helpers ----------

    /** All rows of the given players (one query), grouped by player, namespaces ascending. */
    private static Map<UUID, List<Row>> playerRows(Connection c, List<UUID> players) throws SQLException {
        Map<UUID, List<Row>> out = new LinkedHashMap<>();
        if (players.isEmpty()) return out;
        Array ids = c.createArrayOf("uuid", players.toArray());
        try (PreparedStatement ps = prepare(c,
                        "SELECT player, namespace, data::text, version FROM pantheon_player WHERE player = ANY(?)"
                                + " ORDER BY player, namespace",
                        List.of(ids));
                ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                UUID id = rs.getObject(1, UUID.class);
                String ns = rs.getString(2);
                out.computeIfAbsent(id, k -> new ArrayList<>())
                        .add(Row.newBuilder()
                                .setRef(playerRef(id, ns))
                                .setVersion(rs.getLong(4))
                                .putAllData(JsonValues.parseObject(rs.getString(3)))
                                .build());
            }
        }
        return out;
    }

    private static RowRef playerRef(UUID player, String namespace) {
        return RowRef.newBuilder()
                .setPlayerRow(PlayerRow.newBuilder().setPlayer(player.toString()).setNamespace(namespace))
                .build();
    }

    /** A store row from {@code id, data::text, version}. */
    private static Row docRow(String store, ResultSet rs) throws SQLException {
        return Row.newBuilder()
                .setRef(RowRef.newBuilder().setStoreDoc(StoreDoc.newBuilder().setStore(store).setId(rs.getString(1))))
                .setVersion(rs.getLong(3))
                .putAllData(JsonValues.parseObject(rs.getString(2)))
                .build();
    }

    /** {@code d.store = '<store>' AND <not expired> AND <conditions>} with its parameters. */
    private static SqlFragment storeWhere(String store, List<Condition> where) {
        List<SqlFragment> parts = new ArrayList<>();
        parts.add(SqlFragment.of("d.store = " + SqlLiteral.store(store)));
        parts.add(SqlFragment.of(NOT_EXPIRED));
        for (Condition cond : where) parts.add(PredicateSql.where("d", cond));
        return SqlFragment.join(" AND ", parts);
    }

    private static UUID uuid(String s) {
        try {
            return UUID.fromString(s);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("invalid player UUID: " + s);
        }
    }

    private static PreparedStatement prepare(Connection c, String sql, List<?> params) throws SQLException {
        PreparedStatement ps = c.prepareStatement(sql);
        ps.setQueryTimeout(QUERY_TIMEOUT_SECONDS);
        for (int i = 0; i < params.size(); i++) ps.setObject(i + 1, params.get(i));
        return ps;
    }

    /**
     * {@code FROM pantheon_player p0 LEFT JOIN pantheon_player p_<ns> ON … WHERE p0.namespace = 'pantheon' AND …}
     * with one join per namespace used in conditions or sorts (E9, R4 literals).
     */
    private record PlayerFrom(SqlFragment fromWhere, Function<FieldSpec, String> aliases) {

        static PlayerFrom of(List<Condition> where, List<Sort> sort) {
            Set<String> ns = new LinkedHashSet<>();
            for (Condition c : where) ns.add(c.getField().getNamespace());
            for (Sort s : sort) ns.add(s.getField().getNamespace());
            ns.remove(PANTHEON);
            StringBuilder sb = new StringBuilder("FROM pantheon_player p0");
            for (String n : ns) {
                sb.append(" LEFT JOIN pantheon_player p_").append(n).append(" ON p_").append(n)
                        .append(".player = p0.player AND p_").append(n).append(".namespace = ")
                        .append(SqlLiteral.namespace(n));
            }
            sb.append(" WHERE p0.namespace = ").append(SqlLiteral.namespace(PANTHEON));
            Function<FieldSpec, String> aliases =
                    f -> f.getNamespace().equals(PANTHEON) ? "p0" : "p_" + f.getNamespace();
            List<SqlFragment> parts = new ArrayList<>();
            parts.add(SqlFragment.of(sb.toString()));
            for (Condition c : where) {
                parts.add(SqlFragment.of(" AND "));
                parts.add(PredicateSql.where(aliases.apply(c.getField()), c));
            }
            return new PlayerFrom(SqlFragment.concat(parts.toArray(SqlFragment[]::new)), aliases);
        }

        String aliasFor(FieldSpec f) {
            return aliases.apply(f);
        }
    }
}
