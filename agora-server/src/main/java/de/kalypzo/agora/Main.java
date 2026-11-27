package de.kalypzo.agora;

import de.kalypzo.agora.config.AgoraConfig;
import java.util.concurrent.CountDownLatch;

public final class Main {
    private Main() {}

    public static void main(String[] args) throws InterruptedException {
        AgoraServer server = AgoraServer.start(AgoraConfig.fromEnv(System.getenv()));
        Runtime.getRuntime().addShutdownHook(shutdownHook(server));
        new CountDownLatch(1).await(); // gRPC threads are daemons: keep main alive until SIGTERM
    }

    /** SIGTERM/SIGINT: NOT_SERVING, close the node streams with UNAVAILABLE, drain (10 s), close the pool. */
    static Thread shutdownHook(AgoraServer server) {
        return new Thread(server::close, "agora-shutdown");
    }
}
