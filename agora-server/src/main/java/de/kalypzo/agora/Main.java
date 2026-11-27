package de.kalypzo.agora;

import de.kalypzo.agora.config.AgoraConfig;

public final class Main {
    private Main() {}

    public static void main(String[] args) {
        AgoraServer.start(AgoraConfig.fromEnv(System.getenv()));
        // Shutdown hook is added in A-14.
    }
}
