package de.kalypzo.agora.service;

import io.grpc.stub.StreamObserver;

/** Implementation of one unary RPC. */
@FunctionalInterface
public interface RpcHandler<Q, R> {
    void handle(Q request, StreamObserver<R> response);
}
