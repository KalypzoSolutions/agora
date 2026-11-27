package de.kalypzo.agora.config;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

/**
 * Server configuration. Can be built from the environment ({@link #fromEnv}) or directly, e.g. by in-process tests.
 *
 * @param paperWelcomeDelay minimum time after start before PAPER nodes receive {@code Welcome} (E11, R1)
 */
public record AgoraConfig(
        int port,
        String dbUrl,
        String dbUser,
        String dbPassword,
        int dbPoolSize,
        Path tokensFile,
        Duration paperWelcomeDelay) {

    public static final int DEFAULT_PORT = 9090;
    public static final int DEFAULT_POOL_SIZE = 16;
    public static final Duration DEFAULT_PAPER_WELCOME_DELAY = Duration.ofSeconds(15);

    /** Convenience constructor with the default {@code paperWelcomeDelay}. */
    public AgoraConfig(int port, String dbUrl, String dbUser, String dbPassword, int dbPoolSize, Path tokensFile) {
        this(port, dbUrl, dbUser, dbPassword, dbPoolSize, tokensFile, DEFAULT_PAPER_WELCOME_DELAY);
    }

    public static AgoraConfig fromEnv(Map<String, String> env) {
        return new AgoraConfig(
                intVar(env, "AGORA_PORT", DEFAULT_PORT),
                required(env, "AGORA_DB_URL"),
                required(env, "AGORA_DB_USER"),
                required(env, "AGORA_DB_PASSWORD"),
                intVar(env, "AGORA_DB_POOL_SIZE", DEFAULT_POOL_SIZE),
                Path.of(required(env, "AGORA_TOKENS_FILE")),
                Duration.ofMillis(intVar(env, "AGORA_PAPER_WELCOME_DELAY_MS", (int) DEFAULT_PAPER_WELCOME_DELAY.toMillis())));
    }

    private static String required(Map<String, String> env, String name) {
        String v = env.get(name);
        if (v == null || v.isBlank()) {
            throw new IllegalArgumentException("Missing required environment variable " + name);
        }
        return v;
    }

    private static int intVar(Map<String, String> env, String name, int def) {
        String v = env.get(name);
        if (v == null || v.isBlank()) return def;
        try {
            return Integer.parseInt(v.trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Environment variable " + name + " is not a number: " + v);
        }
    }

    @Override
    public String toString() {
        return "AgoraConfig[port=" + port + ", dbUrl=" + dbUrl + ", dbUser=" + dbUser + ", dbPassword=***, dbPoolSize="
                + dbPoolSize + ", tokensFile=" + tokensFile + ", paperWelcomeDelay=" + paperWelcomeDelay + "]";
    }
}
