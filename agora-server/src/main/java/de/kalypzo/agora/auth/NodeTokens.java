package de.kalypzo.agora.auth;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/** Node tokens from a file: one {@code node_id=token} per line, {@code #} starts a comment (E20). */
public final class NodeTokens {

    private static final byte[] DUMMY = new byte[32];

    private final Map<String, byte[]> tokens;

    private NodeTokens(Map<String, byte[]> tokens) {
        this.tokens = tokens;
    }

    public static NodeTokens load(Path file) {
        List<String> lines;
        try {
            lines = Files.readAllLines(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read tokens file " + file, e);
        }
        Map<String, byte[]> map = new HashMap<>();
        for (int i = 0; i < lines.size(); i++) {
            String line = lines.get(i).strip();
            if (line.isEmpty() || line.startsWith("#")) continue;
            int eq = line.indexOf('=');
            String node = eq < 0 ? "" : line.substring(0, eq).strip();
            String token = eq < 0 ? "" : line.substring(eq + 1).strip();
            // Never put the line itself into the message: it contains a token.
            if (node.isEmpty() || token.isEmpty()) {
                throw new IllegalArgumentException("Invalid line " + (i + 1) + " in tokens file " + file);
            }
            if (map.put(node, token.getBytes(StandardCharsets.UTF_8)) != null) {
                throw new IllegalArgumentException("Duplicate node '" + node + "' in tokens file " + file);
            }
        }
        return new NodeTokens(map);
    }

    /** Returns the node id if the token matches (constant-time comparison). */
    public Optional<String> authenticate(String nodeId, String token) {
        if (nodeId == null || token == null) return Optional.empty();
        byte[] expected = tokens.get(nodeId);
        byte[] given = token.getBytes(StandardCharsets.UTF_8);
        boolean ok = MessageDigest.isEqual(expected != null ? expected : DUMMY, given) && expected != null;
        return ok ? Optional.of(nodeId) : Optional.empty();
    }

    @Override
    public String toString() {
        return "NodeTokens[nodes=" + tokens.keySet() + "]";
    }
}
