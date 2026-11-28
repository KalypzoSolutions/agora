package de.kalypzo.agora.config;

import java.nio.file.Path;
import java.time.Duration;
import java.util.Map;

/**
 * Server configuration. Can be built from the environment ({@link #fromEnv}) or directly, e.g. by in-process tests.
 *
 * @param paperWelcomeDelay minimum time after start before PAPER nodes receive {@code Welcome} (E11, R1)
 * @param teleport          time limits of the network teleport state machine
 */
public record AgoraConfig(
        int port,
        String dbUrl,
        String dbUser,
        String dbPassword,
        int dbPoolSize,
        Path tokensFile,
        Duration paperWelcomeDelay,
        TeleportTimeouts teleport) {

    /**
     * Longest waits during a network teleport.
     *
     * @param prepare  {@code PrepareArrival} until {@code ArrivalPrepared}
     * @param connect  {@code ConnectPlayer} until {@code ConnectResult}
     * @param arrival  {@code ConnectPlayer} sent (or {@code ALREADY_HERE}) until {@code PlayerArrived}
     * @param total    whole teleport from receipt
     */
    public record TeleportTimeouts(Duration prepare, Duration connect, Duration arrival, Duration total) {
        public static final TeleportTimeouts DEFAULT = new TeleportTimeouts(
                Duration.ofSeconds(3), Duration.ofSeconds(10), Duration.ofSeconds(25), Duration.ofSeconds(38));

        public TeleportTimeouts {
            positive(prepare, "prepare");
            positive(connect, "connect");
            positive(arrival, "arrival");
            positive(total, "total");
        }

        private static void positive(Duration d, String name) {
            if (d == null || d.isZero() || d.isNegative()) {
                throw new IllegalArgumentException("Teleport timeout " + name + " must be positive: " + d);
            }
        }
    }

    public static final int DEFAULT_PORT = 9090;
    public static final int DEFAULT_POOL_SIZE = 16;
    public static final Duration DEFAULT_PAPER_WELCOME_DELAY = Duration.ofSeconds(15);

    /** Convenience constructor with the default {@code paperWelcomeDelay} and teleport timeouts. */
    public AgoraConfig(int port, String dbUrl, String dbUser, String dbPassword, int dbPoolSize, Path tokensFile) {
        this(port, dbUrl, dbUser, dbPassword, dbPoolSize, tokensFile, DEFAULT_PAPER_WELCOME_DELAY);
    }

    /** Convenience constructor with the default teleport timeouts. */
    public AgoraConfig(int port, String dbUrl, String dbUser, String dbPassword, int dbPoolSize, Path tokensFile,
                       Duration paperWelcomeDelay) {
        this(port, dbUrl, dbUser, dbPassword, dbPoolSize, tokensFile, paperWelcomeDelay, TeleportTimeouts.DEFAULT);
    }

    public static AgoraConfig fromEnv(Map<String, String> env) {
        return new AgoraConfig(
                intVar(env, "AGORA_PORT", DEFAULT_PORT),
                required(env, "AGORA_DB_URL"),
                required(env, "AGORA_DB_USER"),
                required(env, "AGORA_DB_PASSWORD"),
                intVar(env, "AGORA_DB_POOL_SIZE", DEFAULT_POOL_SIZE),
                Path.of(required(env, "AGORA_TOKENS_FILE")),
                Duration.ofMillis(intVar(env, "AGORA_PAPER_WELCOME_DELAY_MS", (int) DEFAULT_PAPER_WELCOME_DELAY.toMillis())),
                new TeleportTimeouts(
                        ms(env, "AGORA_TELEPORT_PREPARE_MS", TeleportTimeouts.DEFAULT.prepare()),
                        ms(env, "AGORA_TELEPORT_CONNECT_MS", TeleportTimeouts.DEFAULT.connect()),
                        ms(env, "AGORA_TELEPORT_ARRIVAL_MS", TeleportTimeouts.DEFAULT.arrival()),
                        ms(env, "AGORA_TELEPORT_TOTAL_MS", TeleportTimeouts.DEFAULT.total())));
    }

    private static String required(Map<String, String> env, String name) {
        String v = env.get(name);
        if (v == null || v.isBlank()) {
            throw new IllegalArgumentException("Missing required environment variable " + name);
        }
        return v;
    }

    private static Duration ms(Map<String, String> env, String name, Duration def) {
        return Duration.ofMillis(intVar(env, name, (int) def.toMillis()));
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
                + dbPoolSize + ", tokensFile=" + tokensFile + ", paperWelcomeDelay=" + paperWelcomeDelay + ", teleport=[prepare="
                + teleport.prepare() + ", connect=" + teleport.connect() + ", arrival=" + teleport.arrival()
                + ", total=" + teleport.total() + "]]";
    }
}
