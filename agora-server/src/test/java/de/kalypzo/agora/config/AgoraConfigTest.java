package de.kalypzo.agora.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;

class AgoraConfigTest {

    private static Map<String, String> base() {
        Map<String, String> env = new HashMap<>();
        env.put("AGORA_DB_URL", "jdbc:postgresql://localhost/agora");
        env.put("AGORA_DB_USER", "agora");
        env.put("AGORA_DB_PASSWORD", "s3cret-pw");
        env.put("AGORA_TOKENS_FILE", "/etc/agora/tokens");
        return env;
    }

    @Test
    void missingDbUrlNamesVariable() {
        Map<String, String> env = base();
        env.remove("AGORA_DB_URL");
        assertThatThrownBy(() -> AgoraConfig.fromEnv(env))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("AGORA_DB_URL");
    }

    @Test
    void defaultsApply() {
        AgoraConfig c = AgoraConfig.fromEnv(base());
        assertThat(c.port()).isEqualTo(9090);
        assertThat(c.dbPoolSize()).isEqualTo(16);
        assertThat(c.paperWelcomeDelay()).isEqualTo(Duration.ofSeconds(15));
        assertThat(c.tokensFile()).isEqualTo(Path.of("/etc/agora/tokens"));
    }

    @Test
    void overridesApply() {
        Map<String, String> env = base();
        env.put("AGORA_PORT", "1234");
        env.put("AGORA_DB_POOL_SIZE", "4");
        AgoraConfig c = AgoraConfig.fromEnv(env);
        assertThat(c.port()).isEqualTo(1234);
        assertThat(c.dbPoolSize()).isEqualTo(4);
    }

    @Test
    void invalidNumberNamesVariable() {
        Map<String, String> env = base();
        env.put("AGORA_PORT", "abc");
        assertThatThrownBy(() -> AgoraConfig.fromEnv(env))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("AGORA_PORT");
    }

    @Test
    void toStringMasksPassword() {
        assertThat(AgoraConfig.fromEnv(base()).toString()).doesNotContain("s3cret-pw").contains("dbUrl");
    }

    @Test
    void teleportTimeoutsDefault() {
        AgoraConfig c = AgoraConfig.fromEnv(base());
        assertThat(c.teleport()).isEqualTo(AgoraConfig.TeleportTimeouts.DEFAULT);
        assertThat(AgoraConfig.TeleportTimeouts.DEFAULT.total()).isEqualTo(Duration.ofSeconds(38));
    }

    @Test
    void teleportTimeoutsFromEnv() {
        Map<String, String> env = base();
        env.put("AGORA_TELEPORT_ARRIVAL_MS", "2500");
        assertThat(AgoraConfig.fromEnv(env).teleport().arrival()).isEqualTo(Duration.ofMillis(2500));
    }

    @Test
    void teleportTimeoutsBeyondPantheonLimitsWarnOnce() {
        Map<String, String> env = base();
        env.put("AGORA_TELEPORT_ARRIVAL_MS", "33000"); // 3 s + 33 s >= 35 s
        env.put("AGORA_TELEPORT_TOTAL_MS", "40000");
        assertThat(warnings(env)).singleElement().asString().contains("35", "40");
    }

    @Test
    void defaultTeleportTimeoutsDoNotWarn() {
        assertThat(warnings(base())).isEmpty();
    }

    private static List<String> warnings(Map<String, String> env) {
        Logger logger = (Logger) LoggerFactory.getLogger(AgoraConfig.class);
        ListAppender<ILoggingEvent> appender = new ListAppender<>();
        appender.start();
        logger.addAppender(appender);
        try {
            AgoraConfig.fromEnv(env);
        } finally {
            logger.detachAppender(appender);
        }
        return appender.list.stream().filter(e -> e.getLevel() == Level.WARN).map(ILoggingEvent::getFormattedMessage)
                .toList();
    }

    @Test
    void teleportTimeoutRejectsZero() {
        Map<String, String> env = base();
        env.put("AGORA_TELEPORT_PREPARE_MS", "0");
        assertThatThrownBy(() -> AgoraConfig.fromEnv(env)).isInstanceOf(IllegalArgumentException.class);
    }
}
