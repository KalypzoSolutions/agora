package de.kalypzo.agora.presence;

import de.kalypzo.agora.proto.v1.PresenceEntry;

/**
 * Real presence events, for the audit log (A-19). Reconciliation from {@code Hello} and {@code Heartbeat} fires
 * nothing (audit_log.md §5: "Abgleiche erzeugen keine Einträge"), and a claim alone is no event (the join is the
 * first switch, {@code previousServer} empty). Called after the change is applied and broadcast, outside of the
 * presence lock, on a lane thread, a common-pool thread (claim after a kick wait) or the node-gone lane:
 * implementations must not block (A-19: hand the audit write to its own executor).
 */
public interface PresenceEvents {

    PresenceEvents NOOP = new PresenceEvents() {};

    /** The owning proxy moved the player to {@code entry.server}; {@code previousServer} is "" for the first one. */
    default void switched(PresenceEntry entry, String previousServer) {}

    /** {@code entry} (as it was) was removed. */
    default void removed(PresenceEntry entry, RemovalReason reason) {}
}
