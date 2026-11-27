package de.kalypzo.agora.grpc;

import de.kalypzo.agora.auth.NodeIdentity;
import de.kalypzo.agora.node.NodeRegistry;
import de.kalypzo.agora.node.NodeSession;
import de.kalypzo.agora.presence.PresenceService;
import de.kalypzo.agora.proto.v1.ClaimPresenceResponse;
import de.kalypzo.agora.proto.v1.NodeKind;
import de.kalypzo.agora.service.AgoraService;
import io.grpc.Status;
import java.util.UUID;
import java.util.concurrent.CompletionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Registers {@code ClaimPresence} (A-11, R5) on the {@link AgoraService}. */
public final class PresenceEndpoints {

    private static final Logger LOG = LoggerFactory.getLogger(PresenceEndpoints.class);

    private PresenceEndpoints() {}

    /**
     * {@code ClaimPresence}: only a VELOCITY node with an open {@code Connect} stream may claim. Without an open
     * session {@code FAILED_PRECONDITION}, from a PAPER node {@code PERMISSION_DENIED}, without entry or with an
     * invalid UUID {@code INVALID_ARGUMENT}. The answer may wait for the old proxy's kick answer (at most the kick
     * timeout); the handler itself does not block.
     */
    public static void register(AgoraService service, PresenceService presence, NodeRegistry registry) {
        service.onClaimPresence((req, out) -> {
            NodeIdentity caller = NodeIdentity.CONTEXT_KEY.get();
            if (caller == null) {
                out.onError(Status.UNAUTHENTICATED.withDescription("no node identity").asRuntimeException());
                return;
            }
            if (registry.isShuttingDown()) {
                out.onError(Status.UNAVAILABLE.withDescription("shutting down").asRuntimeException());
                return;
            }
            NodeSession session = registry.get(caller.nodeId()).filter(NodeSession::isOpen).orElse(null);
            if (session == null) {
                out.onError(Status.FAILED_PRECONDITION.withDescription("no open Connect stream for node '"
                        + caller.nodeId() + "'").asRuntimeException());
                return;
            }
            if (session.kind() != NodeKind.VELOCITY) {
                out.onError(Status.PERMISSION_DENIED.withDescription("only VELOCITY nodes claim presence")
                        .asRuntimeException());
                return;
            }
            if (!req.hasEntry()) {
                out.onError(Status.INVALID_ARGUMENT.withDescription("entry is required").asRuntimeException());
                return;
            }
            try {
                UUID.fromString(req.getEntry().getPlayer());
            } catch (IllegalArgumentException e) {
                out.onError(Status.INVALID_ARGUMENT.withDescription("entry.player is not a UUID").asRuntimeException());
                return;
            }
            presence.claim(caller.nodeId(), req.getEntry()).whenComplete((accepted, error) -> {
                if (error == null) {
                    out.onNext(ClaimPresenceResponse.newBuilder().setAccepted(accepted).build());
                    out.onCompleted();
                    return;
                }
                Throwable cause = error instanceof CompletionException && error.getCause() != null
                        ? error.getCause() : error;
                if (cause instanceof IllegalStateException) {
                    out.onError(Status.FAILED_PRECONDITION.withDescription(cause.getMessage()).asRuntimeException());
                } else {
                    LOG.error("ClaimPresence of {} by '{}' failed", req.getEntry().getPlayer(), caller.nodeId(), cause);
                    out.onError(Status.INTERNAL.withDescription("internal error").asRuntimeException());
                }
            });
        });
    }
}
