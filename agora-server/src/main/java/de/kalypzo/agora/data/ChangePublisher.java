package de.kalypzo.agora.data;

import de.kalypzo.agora.proto.v1.DocChanged;

/**
 * Receives every committed change of a row (implemented by the holder routing, A-10). Called <b>after</b> the commit,
 * in commit order per request, with the {@code idempotency_key} of the request that caused it (E4). Must not block.
 */
public interface ChangePublisher {

    /** Drops every change, until the holder routing is wired in. */
    ChangePublisher NOOP = change -> {};

    void publish(DocChanged change);
}
