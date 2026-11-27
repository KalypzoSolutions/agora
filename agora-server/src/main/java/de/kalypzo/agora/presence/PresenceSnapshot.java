package de.kalypzo.agora.presence;

import de.kalypzo.agora.proto.v1.PresenceEntry;
import java.util.List;

/** The complete current presence, sent with every {@code Welcome} (implemented by A-11). */
@FunctionalInterface
public interface PresenceSnapshot {

    /** No presence, until the presence service is wired in. */
    PresenceSnapshot EMPTY = List::of;

    List<PresenceEntry> snapshot();
}
