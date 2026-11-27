package de.kalypzo.agora.grpc;

import de.kalypzo.agora.audit.AuditIngest;
import de.kalypzo.agora.audit.TimelineService;
import de.kalypzo.agora.auth.NodeIdentity;
import de.kalypzo.agora.proto.v1.AppendAuditResponse;
import de.kalypzo.agora.proto.v1.TimelineResponse;
import de.kalypzo.agora.service.AgoraService;
import io.grpc.Status;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Registers {@code AppendAudit} (A-17) and {@code Timeline} (A-18) on the {@link AgoraService}. */
public final class AuditEndpoints {

    private static final Logger LOG = LoggerFactory.getLogger(AuditEndpoints.class);

    private AuditEndpoints() {}

    /** The answer follows the commit; a database failure answers {@code UNAVAILABLE} (Pantheon sends again). */
    public static void register(AgoraService service, AuditIngest ingest) {
        service.onAppendAudit((bundle, out) -> {
            NodeIdentity caller = NodeIdentity.CONTEXT_KEY.get();
            if (caller == null) {
                out.onError(Status.UNAUTHENTICATED.withDescription("no node identity").asRuntimeException());
                return;
            }
            try {
                ingest.append(caller.nodeId(), bundle);
            } catch (SQLException | RuntimeException e) {
                LOG.error("AppendAudit from {} failed", caller.nodeId(), e);
                out.onError(Status.UNAVAILABLE.withDescription("audit storage unavailable").asRuntimeException());
                return;
            }
            out.onNext(AppendAuditResponse.getDefaultInstance());
            out.onCompleted();
        });
    }

    /**
     * Invalid requests (no viewer, empty viewer name, bad pattern or UUID) answer {@code INVALID_ARGUMENT}; if the
     * {@code audit_view} entry or the read fails, {@code UNAVAILABLE} (E31).
     */
    public static void timeline(AgoraService service, TimelineService timeline) {
        service.onTimeline((request, out) -> {
            NodeIdentity caller = NodeIdentity.CONTEXT_KEY.get();
            if (caller == null) {
                out.onError(Status.UNAUTHENTICATED.withDescription("no node identity").asRuntimeException());
                return;
            }
            TimelineResponse response;
            try {
                response = timeline.timeline(caller.nodeId(), request);
            } catch (IllegalArgumentException e) {
                out.onError(Status.INVALID_ARGUMENT.withDescription(e.getMessage()).asRuntimeException());
                return;
            } catch (SQLException | RuntimeException e) {
                LOG.error("Timeline for {} failed", caller.nodeId(), e);
                out.onError(Status.UNAVAILABLE.withDescription("audit storage unavailable").asRuntimeException());
                return;
            }
            out.onNext(response);
            out.onCompleted();
        });
    }
}
