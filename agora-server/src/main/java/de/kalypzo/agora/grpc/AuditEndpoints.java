package de.kalypzo.agora.grpc;

import de.kalypzo.agora.audit.AuditIngest;
import de.kalypzo.agora.auth.NodeIdentity;
import de.kalypzo.agora.proto.v1.AppendAuditResponse;
import de.kalypzo.agora.service.AgoraService;
import io.grpc.Status;
import java.sql.SQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Registers {@code AppendAudit} (A-17) on the {@link AgoraService}. */
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
}
