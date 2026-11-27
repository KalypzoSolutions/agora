package de.kalypzo.agora.presence;

import de.kalypzo.agora.proto.v1.PresenceEntry;
import java.util.List;
import java.util.function.Consumer;

/** The complete current presence, sent with every {@code Welcome} (implemented by A-11). */
@FunctionalInterface
public interface PresenceSnapshot {

    /** No presence, until the presence service is wired in. */
    PresenceSnapshot EMPTY = List::of;

    List<PresenceEntry> snapshot();

    /**
     * Hands the snapshot to {@code sender}, which must only queue the {@code Welcome} (no blocking). Implementations
     * that broadcast changes run {@code sender} under the same lock as their broadcasts, so a session sees every
     * change either in its Welcome or after it, never before it (A-11). The default has no broadcasts to order
     * against.
     */
    default void sendSnapshot(Consumer<List<PresenceEntry>> sender) {
        sender.accept(snapshot());
    }
}
