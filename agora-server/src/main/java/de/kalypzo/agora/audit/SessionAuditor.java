package de.kalypzo.agora.audit;

import com.google.protobuf.ByteString;
import de.kalypzo.agora.presence.PresenceEvents;
import de.kalypzo.agora.presence.RemovalReason;
import de.kalypzo.agora.proto.v1.AuditEntry;
import de.kalypzo.agora.proto.v1.AuditValue;
import de.kalypzo.agora.proto.v1.PresenceEntry;
import de.kalypzo.agora.proto.v1.StoredAuditEntry;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes {@code pantheon:join}, {@code switch} and {@code quit} from real presence events (audit_log §5). The
 * {@code server} of every entry is the proxy ({@code entry.proxy}). Without a server there is no join and no quit.
 * Has its own {@link UuidV7.Generator}, so ids follow the injected clock and never the shared monotonic state.
 */
public final class SessionAuditor implements PresenceEvents {

    private static final Logger LOG = LoggerFactory.getLogger(SessionAuditor.class);

    public static final String JOIN = "pantheon:join";
    public static final String SWITCH = "pantheon:switch";
    public static final String QUIT = "pantheon:quit";

    private final AuditSink sink;
    private final Clock clock;
    private final UuidV7.Generator ids = new UuidV7.Generator();

    public SessionAuditor(AuditSink sink, Clock clock) {
        this.sink = sink;
        this.clock = clock;
    }

    @Override
    public void switched(PresenceEntry entry, String previousServer) {
        if (entry.getServer().isEmpty()) return;
        AuditEntry.Builder b = base(entry, previousServer.isEmpty() ? JOIN : SWITCH);
        if (b == null) return;
        if (!previousServer.isEmpty()) b.putWith("from", str(previousServer));
        b.putWith("to", str(entry.getServer()));
        sink.add(stored(entry, b));
    }

    @Override
    public void removed(PresenceEntry entry, RemovalReason reason) {
        if (entry.getServer().isEmpty()) return; // never joined: no quit
        AuditEntry.Builder b = base(entry, QUIT);
        if (b == null) return;
        b.putWith("from", str(entry.getServer()));
        b.putWith("reason", str(switch (reason) {
            case LEFT -> "left";
            case REPLACED -> "replaced";
            case PROXY_LOST -> "proxy_lost";
        }));
        sink.add(stored(entry, b));
    }

    private AuditEntry.Builder base(PresenceEntry entry, String action) {
        UUID player;
        try {
            player = UUID.fromString(entry.getPlayer());
        } catch (IllegalArgumentException e) {
            LOG.warn("Session audit skipped: invalid player '{}'", entry.getPlayer());
            return null;
        }
        return AuditEntry.newBuilder()
                .setId(bytes(ids.next(clock)))
                .setPlayer(bytes(player))
                .setAction(action);
    }

    private static StoredAuditEntry stored(PresenceEntry entry, AuditEntry.Builder b) {
        return StoredAuditEntry.newBuilder().setEntry(b).setServer(entry.getProxy()).build();
    }

    private static AuditValue str(String s) {
        return AuditValue.newBuilder().setS(s).build();
    }

    private static ByteString bytes(UUID u) {
        return ByteString.copyFrom(ByteBuffer.allocate(16)
                .putLong(u.getMostSignificantBits()).putLong(u.getLeastSignificantBits()).flip());
    }
}
