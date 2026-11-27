package de.kalypzo.agora.audit;

import com.google.protobuf.ByteString;
import de.kalypzo.agora.proto.v1.AuditEntry;
import de.kalypzo.agora.proto.v1.AuditLocation;
import de.kalypzo.agora.proto.v1.AuditValue;
import de.kalypzo.agora.proto.v1.AuditViewer;
import de.kalypzo.agora.proto.v1.StoredAuditEntry;
import de.kalypzo.agora.proto.v1.TimelineRequest;
import de.kalypzo.agora.proto.v1.TimelineResponse;
import java.nio.ByteBuffer;
import java.sql.Array;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;
import javax.sql.DataSource;

/**
 * Reads a player's audit timeline (audit_log §9, §10) and logs every first page as {@code pantheon:audit_view}
 * before reading it (E31). Invalid requests throw {@link IllegalArgumentException}, storage failures
 * {@link SQLException}.
 */
public final class TimelineService {

    public static final String AUDIT_VIEW = "pantheon:audit_view";
    static final int DEFAULT_LIMIT = 100;
    static final int MAX_LIMIT = 500;
    static final Duration DEFAULT_RANGE = Duration.ofHours(24);
    static final UUID NIL = new UUID(0, 0);
    /** Range bound not applied (only with a cursor, R48). */
    static final long UNBOUNDED = -1;
    /** Largest instant whose millis fit into the 48 bit of a UUIDv7. */
    private static final long MAX_MILLIS = (1L << 48) - 1;

    private static final Pattern EXACT = Pattern.compile("[a-z0-9_]+:[a-z0-9_]+");
    private static final Pattern NAMESPACE = Pattern.compile("([a-z0-9_]+):\\*");

    private final AuditStore store;
    private final DataSource ds;
    private final Clock clock;
    /** Own generator: a test clock must not move the shared one (and with it other IDs) into its future. */
    private final UuidV7.Generator ids = new UuidV7.Generator();

    public TimelineService(AuditStore store, DataSource ds, Clock clock) {
        this.store = store;
        this.ds = ds;
        this.clock = clock;
    }

    /** One page; without cursor the {@code audit_view} entry is committed first, otherwise nothing is read. */
    public TimelineResponse timeline(String nodeId, TimelineRequest req) throws SQLException {
        Query q = query(req, null);
        UUID view = null;
        if (!req.hasCursor()) {
            view = ids.next(clock);
            store.insert(List.of(auditView(view, nodeId, req, q)));
            q = q.excluding(view); // R16: the view of this request is not on its own page
        }
        TimelineResponse.Builder out = TimelineResponse.newBuilder();
        try (Connection c = ds.getConnection(); PreparedStatement ps = c.prepareStatement(q.sql())) {
            q.bind(c, ps);
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) out.addEntries(row(rs));
            }
        }
        if (out.getEntriesCount() == q.limit()) {
            out.setCursor(out.getEntries(q.limit() - 1).getEntry().getId());
        }
        return out.build();
    }

    private static StoredAuditEntry auditView(UUID id, String nodeId, TimelineRequest req, Query q) {
        AuditViewer v = req.getViewer();
        AuditEntry.Builder e = AuditEntry.newBuilder()
                .setId(bytes(id))
                .setPlayer(v.hasPlayer() ? v.getPlayer() : bytes(NIL))
                .setAction(AUDIT_VIEW)
                .setTarget(req.getPlayer())
                .putWith("viewer_name", str(v.getName()))
                .putWith("via", str(v.getVia()))
                .putWith("from", AuditValue.newBuilder().setL(q.fromMillis()).build())
                .putWith("to", AuditValue.newBuilder().setL(q.toMillis()).build());
        if (req.getActionsCount() > 0) e.putWith("actions", str(String.join(",", req.getActionsList())));
        if (req.hasTrace()) e.putWith("trace", str(uuid(req.getTrace()).toString()));
        return StoredAuditEntry.newBuilder().setEntry(e).setServer(nodeId).build();
    }

    /**
     * Validates the request and builds the §9 query. The range bounds are inlined as UUID literals so that
     * Postgres prunes the partitions at plan time (R4); everything else is bound.
     */
    Query query(TimelineRequest req, UUID exclude) {
        uuid16(req.getPlayer(), "player");
        if (!req.hasViewer()) throw new IllegalArgumentException("viewer fehlt");
        AuditViewer v = req.getViewer();
        if (v.getName().isEmpty()) throw new IllegalArgumentException("viewer.name fehlt");
        if (v.hasPlayer()) uuid16(v.getPlayer(), "viewer.player");
        if (req.hasCursor()) uuid16(req.getCursor(), "cursor");
        if (req.hasTrace()) uuid16(req.getTrace(), "trace");
        checkMillis(req.getFromMillis(), "from_millis");
        checkMillis(req.getToMillis(), "to_millis");

        // R26: 0 = unset; `to` defaults to now, `from` to `to` - 24 h.
        // R48: with a cursor, an unset bound stays open (the cursor is the upper bound, retention the lower one);
        // otherwise a slowly paged first result would lose [now1 - 24 h, now2 - 24 h).
        long to;
        long from;
        if (req.hasCursor()) {
            to = req.getToMillis() != 0 ? req.getToMillis() : UNBOUNDED;
            from = req.getFromMillis() != 0 ? req.getFromMillis() : UNBOUNDED;
        } else {
            to = req.getToMillis() != 0 ? req.getToMillis() : clock.millis();
            from = req.getFromMillis() != 0 ? req.getFromMillis() : Math.max(0, to - DEFAULT_RANGE.toMillis());
        }

        List<String> exact = new ArrayList<>();
        List<String> prefixes = new ArrayList<>();
        for (String a : req.getActionsList()) {
            var ns = NAMESPACE.matcher(a);
            if (EXACT.matcher(a).matches()) exact.add(a);
            else if (ns.matches()) prefixes.add(ns.group(1).replace("_", "\\_") + ":%");
            else throw new IllegalArgumentException("ungültiges Aktionsmuster: " + a);
        }

        long rawLimit = Integer.toUnsignedLong(req.getLimit());
        int limit = rawLimit == 0 ? DEFAULT_LIMIT : (int) Math.min(rawLimit, MAX_LIMIT);

        return new Query(uuid(req.getPlayer()), from, to, exact, prefixes,
                req.hasTrace() ? uuid(req.getTrace()) : null,
                req.hasCursor() ? uuid(req.getCursor()) : null, exclude, limit);
    }

    /** A built timeline query; {@link #sql()} and {@link #bind} belong together. */
    record Query(UUID player, long fromMillis, long toMillis, List<String> exact, List<String> prefixes,
            UUID trace, UUID cursor, UUID exclude, int limit) {

        Query excluding(UUID id) {
            return new Query(player, fromMillis, toMillis, exact, prefixes, trace, cursor, id, limit);
        }

        String sql() {
            StringBuilder sb = new StringBuilder("SELECT id, player, target, trace, server, action, world, x, y, z,"
                    + " w.w_keys, w.w_types, w.w_values FROM pantheon_audit"
                    + " CROSS JOIN LATERAL (SELECT array_agg(key ORDER BY key) AS w_keys,"
                    + " array_agg(jsonb_typeof(value) ORDER BY key) AS w_types,"
                    + " array_agg(value #>> '{}' ORDER BY key) AS w_values FROM jsonb_each(with_data)) w"
                    + " WHERE (player = ? OR target = ?)");
            // UUID.toString is [0-9a-f-] only: safe as a literal
            if (fromMillis != UNBOUNDED) {
                sb.append(" AND id >= '").append(UuidV7.lowerBound(Instant.ofEpochMilli(fromMillis))).append("'::uuid");
            }
            if (toMillis != UNBOUNDED) {
                sb.append(" AND id < '").append(UuidV7.lowerBound(Instant.ofEpochMilli(toMillis))).append("'::uuid");
            }
            if (!exact.isEmpty() && !prefixes.isEmpty()) sb.append(" AND (action = ANY(?) OR action LIKE ANY(?))");
            else if (!exact.isEmpty()) sb.append(" AND action = ANY(?)");
            else if (!prefixes.isEmpty()) sb.append(" AND action LIKE ANY(?)");
            if (trace != null) sb.append(" AND trace = ?");
            if (cursor != null) sb.append(" AND id < ?");
            if (exclude != null) sb.append(" AND id <> ?");
            return sb.append(" ORDER BY id DESC LIMIT ").append(limit).toString();
        }

        void bind(Connection c, PreparedStatement ps) throws SQLException {
            int i = 1;
            ps.setObject(i++, player);
            ps.setObject(i++, player);
            if (!exact.isEmpty()) ps.setArray(i++, c.createArrayOf("text", exact.toArray()));
            if (!prefixes.isEmpty()) ps.setArray(i++, c.createArrayOf("text", prefixes.toArray()));
            if (trace != null) ps.setObject(i++, trace);
            if (cursor != null) ps.setObject(i++, cursor);
            if (exclude != null) ps.setObject(i++, exclude);
        }
    }

    private static StoredAuditEntry row(ResultSet rs) throws SQLException {
        AuditEntry.Builder e = AuditEntry.newBuilder()
                .setId(bytes(rs.getObject("id", UUID.class)))
                .setPlayer(bytes(rs.getObject("player", UUID.class)))
                .setAction(rs.getString("action"));
        UUID target = rs.getObject("target", UUID.class);
        if (target != null) e.setTarget(bytes(target));
        UUID trace = rs.getObject("trace", UUID.class);
        if (trace != null) e.setTrace(bytes(trace));
        String world = rs.getString("world");
        if (world != null) {
            e.setLocation(AuditLocation.newBuilder().setWorld(world)
                    .setX(rs.getDouble("x")).setY(rs.getDouble("y")).setZ(rs.getDouble("z")));
        }
        String[] keys = strings(rs.getArray("w_keys"));
        String[] types = strings(rs.getArray("w_types"));
        String[] values = strings(rs.getArray("w_values"));
        for (int i = 0; i < keys.length; i++) {
            e.putWith(keys[i], value(types[i], values[i]));
        }
        return StoredAuditEntry.newBuilder().setEntry(e).setServer(rs.getString("server")).build();
    }

    /** Doubles are always written with a decimal point or exponent (AuditStore.json), longs never. */
    private static AuditValue value(String type, String text) {
        AuditValue.Builder v = AuditValue.newBuilder();
        switch (type) {
            case "string" -> v.setS(text);
            case "boolean" -> v.setB(Boolean.parseBoolean(text));
            case "number" -> {
                if (text.indexOf('.') >= 0 || text.indexOf('e') >= 0 || text.indexOf('E') >= 0) {
                    v.setD(Double.parseDouble(text));
                } else {
                    v.setL(Long.parseLong(text));
                }
            }
            default -> {} // null: value not set
        }
        return v.build();
    }

    private static String[] strings(Array a) throws SQLException {
        return a == null ? new String[0] : (String[]) a.getArray();
    }

    private static AuditValue str(String s) {
        return AuditValue.newBuilder().setS(s).build();
    }

    private static void checkMillis(long millis, String field) {
        if (millis < 0 || millis > MAX_MILLIS) throw new IllegalArgumentException(field + " außerhalb des Bereichs");
    }

    private static void uuid16(ByteString b, String field) {
        if (b.size() != 16) throw new IllegalArgumentException(field + " ist keine 16-Byte-UUID");
    }

    static UUID uuid(ByteString b) {
        return AuditStore.uuid(b);
    }

    static ByteString bytes(UUID u) {
        return ByteString.copyFrom(ByteBuffer.allocate(16).putLong(u.getMostSignificantBits())
                .putLong(u.getLeastSignificantBits()).array());
    }
}
