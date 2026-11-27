package de.kalypzo.agora.data;

import java.util.UUID;

/** Receives the claim "this node now holds this player" (implemented by the holder registry, A-10). */
public interface HolderSink {

    /** Ignores every call, until the holder registry is wired in. */
    HolderSink NOOP = (nodeId, player) -> {};

    /** Called by {@code LoadPlayer} before the rows are read, so that no change is missed in between. */
    void addPlayerHolder(String nodeId, UUID player);
}
