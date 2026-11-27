package de.kalypzo.agora.audit;

import com.google.protobuf.ByteString;
import de.kalypzo.agora.proto.v1.AuditEntry;
import de.kalypzo.agora.proto.v1.AuditValue;
import de.kalypzo.agora.proto.v1.StoredAuditEntry;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.sql.DataSource;

/** Writes audit rows to {@code pantheon_audit} (audit_log §9). */
public class AuditStore {

    private static final String INSERT = "INSERT INTO pantheon_audit"
            + " (id, player, target, trace, server, action, world, x, y, z, with_data)"
            + " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?::jsonb) ON CONFLICT (id) DO NOTHING";

    private final DataSource ds;

    public AuditStore(DataSource ds) {
        this.ds = ds;
    }

    /** Inserts all rows in one transaction (JDBC batch); rows whose id exists are skipped. */
    public void insert(List<StoredAuditEntry> rows) throws SQLException {
        if (rows.isEmpty()) return;
        try (Connection c = ds.getConnection()) {
            c.setAutoCommit(false);
            try (PreparedStatement ps = c.prepareStatement(INSERT)) {
                for (StoredAuditEntry row : rows) {
                    AuditEntry e = row.getEntry();
                    ps.setObject(1, uuid(e.getId()));
                    ps.setObject(2, uuid(e.getPlayer()));
                    ps.setObject(3, e.hasTarget() ? uuid(e.getTarget()) : null, Types.OTHER);
                    ps.setObject(4, e.hasTrace() ? uuid(e.getTrace()) : null, Types.OTHER);
                    ps.setString(5, row.getServer());
                    ps.setString(6, e.getAction());
                    if (e.hasLocation()) {
                        ps.setString(7, e.getLocation().getWorld());
                        ps.setDouble(8, e.getLocation().getX());
                        ps.setDouble(9, e.getLocation().getY());
                        ps.setDouble(10, e.getLocation().getZ());
                    } else {
                        ps.setNull(7, Types.VARCHAR);
                        ps.setNull(8, Types.DOUBLE);
                        ps.setNull(9, Types.DOUBLE);
                        ps.setNull(10, Types.DOUBLE);
                    }
                    ps.setString(11, json(e.getWithMap()));
                    ps.addBatch();
                }
                ps.executeBatch();
                c.commit();
            } catch (SQLException | RuntimeException ex) {
                c.rollback();
                throw ex;
            }
        }
    }

    static UUID uuid(ByteString b) {
        ByteBuffer buf = b.asReadOnlyByteBuffer();
        return new UUID(buf.getLong(), buf.getLong());
    }

    /** JSON object for the {@code with} map; values are validated by {@link AuditIngest} (finite, no NUL). */
    static String json(Map<String, AuditValue> with) {
        StringBuilder sb = new StringBuilder("{");
        boolean first = true;
        for (var en : with.entrySet()) {
            if (!first) sb.append(',');
            first = false;
            quote(sb, en.getKey());
            sb.append(':');
            AuditValue v = en.getValue();
            switch (v.getValueCase()) {
                case S -> quote(sb, v.getS());
                case L -> sb.append(v.getL());
                case D -> sb.append(v.getD());
                case B -> sb.append(v.getB());
                case VALUE_NOT_SET -> sb.append("null");
            }
        }
        return sb.append('}').toString();
    }

    private static void quote(StringBuilder sb, String s) {
        sb.append('"');
        for (int i = 0; i < s.length(); i++) {
            char ch = s.charAt(i);
            switch (ch) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (ch < 0x20) sb.append(String.format("\\u%04x", (int) ch));
                    else sb.append(ch);
                }
            }
        }
        sb.append('"');
    }
}
