package de.kalypzo.agora.auth;

import io.grpc.Context;

/** The authenticated node of the current call. */
public record NodeIdentity(String nodeId) {
    public static final Context.Key<NodeIdentity> CONTEXT_KEY = Context.key("agora-node-identity");
}
