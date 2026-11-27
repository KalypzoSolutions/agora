package de.kalypzo.agora.audit;

import de.kalypzo.agora.proto.v1.AuditBundle;
import de.kalypzo.agora.proto.v1.AuditEntry;
import de.kalypzo.agora.proto.v1.AuditValue;
import de.kalypzo.agora.proto.v1.StoredAuditEntry;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validates an {@link AuditBundle} and stores it (audit_log §7, §8). Invalid entries are dropped one by one and
 * counted like {@code dropped}; the bundle still counts as accepted (E29, E34).
 */
public final class AuditIngest {

    private static final Logger LOG = LoggerFactory.getLogger(AuditIngest.class);
    private static final Pattern ACTION = Pattern.compile("[a-z0-9_]+:[a-z0-9_]+");
    static final Duration MAX_AGE = Duration.ofHours(24);
    static final Duration MAX_AHEAD = Duration.ofHours(1);

    private final AuditStore store;
    private final Clock clock;

    public AuditIngest(AuditStore store, Clock clock) {
        this.store = store;
        this.clock = clock;
    }

    /** @throws SQLException if the rows could not be committed (nothing is stored then) */
    public void append(String nodeId, AuditBundle bundle) throws SQLException {
        Instant now = clock.instant();
        List<StoredAuditEntry> rows = new ArrayList<>(bundle.getEntriesCount());
        long dropped = bundle.getDropped();
        for (AuditEntry e : bundle.getEntriesList()) {
            if (valid(e, now)) {
                rows.add(StoredAuditEntry.newBuilder().setEntry(e).setServer(nodeId).build());
            } else {
                dropped++;
            }
        }
        store.insert(rows);
        if (dropped > 0) {
            LOG.warn("Audit: {} Einträge von {} verworfen", dropped, nodeId);
        }
    }

    private static boolean valid(AuditEntry e, Instant now) {
        if (e.getId().size() != 16 || e.getPlayer().size() != 16) return false;
        if (e.hasTarget() && e.getTarget().size() != 16) return false;
        if (e.hasTrace() && e.getTrace().size() != 16) return false;
        if (!ACTION.matcher(e.getAction()).matches()) return false;
        Instant at = UuidV7.timestamp(AuditStore.uuid(e.getId()));
        if (at.isBefore(now.minus(MAX_AGE)) || at.isAfter(now.plus(MAX_AHEAD))) return false;
        // Postgres cannot store NUL in text/jsonb or NaN/Infinity in JSON: one such entry would fail the whole batch.
        if (e.hasLocation()) {
            if (e.getLocation().getWorld().indexOf('\0') >= 0) return false;
        }
        for (var en : e.getWithMap().entrySet()) {
            if (en.getKey().indexOf('\0') >= 0) return false;
            AuditValue v = en.getValue();
            switch (v.getValueCase()) {
                case S -> {
                    if (v.getS().indexOf('\0') >= 0) return false;
                }
                case D -> {
                    if (!Double.isFinite(v.getD())) return false;
                }
                default -> {}
            }
        }
        return true;
    }
}
