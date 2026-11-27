package de.kalypzo.agora.service;

import de.kalypzo.agora.proto.v1.NodeMessage;
import de.kalypzo.agora.proto.v1.ServiceMessage;
import io.grpc.stub.StreamObserver;

/** Implementation of the bidirectional {@code Connect} stream. */
@FunctionalInterface
public interface ConnectHandler {
    StreamObserver<NodeMessage> connect(StreamObserver<ServiceMessage> out);
}
