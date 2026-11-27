package de.kalypzo.agora.auth;

import io.grpc.Context;
import io.grpc.Contexts;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Authenticates every call (including {@code Connect}) via {@code x-agora-node} and
 * {@code authorization: Bearer <token>}. The gRPC health service is exempt (R27).
 */
public final class AuthInterceptor implements ServerInterceptor {

    private static final Logger LOG = LoggerFactory.getLogger(AuthInterceptor.class);
    static final String HEALTH_SERVICE = "grpc.health.v1.Health";
    static final Metadata.Key<String> NODE = Metadata.Key.of("x-agora-node", Metadata.ASCII_STRING_MARSHALLER);
    static final Metadata.Key<String> AUTH = Metadata.Key.of("authorization", Metadata.ASCII_STRING_MARSHALLER);
    private static final String BEARER = "Bearer ";

    private final NodeTokens tokens;

    public AuthInterceptor(NodeTokens tokens) {
        this.tokens = tokens;
    }

    @Override
    public <Q, R> ServerCall.Listener<Q> interceptCall(
            ServerCall<Q, R> call, Metadata headers, ServerCallHandler<Q, R> next) {
        if (HEALTH_SERVICE.equals(call.getMethodDescriptor().getServiceName())) {
            return next.startCall(call, headers);
        }
        String node = headers.get(NODE);
        String auth = headers.get(AUTH);
        if (node == null || auth == null || !auth.startsWith(BEARER)) {
            return reject(call, "missing credentials");
        }
        Optional<String> id = tokens.authenticate(node, auth.substring(BEARER.length()).strip());
        if (id.isEmpty()) {
            LOG.warn("Authentication failed for node '{}'", node);
            return reject(call, "invalid credentials");
        }
        Context ctx = Context.current().withValue(NodeIdentity.CONTEXT_KEY, new NodeIdentity(id.get()));
        return Contexts.interceptCall(ctx, call, headers, next);
    }

    private static <Q, R> ServerCall.Listener<Q> reject(ServerCall<Q, R> call, String why) {
        call.close(Status.UNAUTHENTICATED.withDescription(why), new Metadata());
        return new ServerCall.Listener<>() {};
    }
}
