package de.kalypzo.agora.node;

import de.kalypzo.agora.proto.v1.Hello;

/**
 * Receives every accepted {@code Hello} ({@code held_players}, {@code watched_stores}, {@code players}), after the
 * session is registered and before the {@code Welcome} is built, on the node's lane (see {@link NodeRegistry}). Used by the holder registry (A-10) and the presence
 * service (A-11).
 */
@FunctionalInterface
public interface HelloListener {
    void onHello(NodeSession session, Hello hello);
}
