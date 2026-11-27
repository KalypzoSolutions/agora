package de.kalypzo.agora.config;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

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
}
