package de.kalypzo.agora.index;

import de.kalypzo.agora.proto.v1.IndexDeclaration;
import de.kalypzo.agora.sql.SqlLiteral;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;

/**
 * Index names (agora.md §7): {@code pantheon_player_<ns>_<field>} and {@code pantheon_doc_<ns>_<store>_<field>}.
 * Names over 63 characters are cut to the first 54 characters + {@code _} + 8 hex characters of the SHA-256 of the
 * full name (E21).
 */
public final class IndexNames {

    static final int MAX = 63;
    private static final int KEEP = 54;

    private IndexNames() {}

    public static String of(IndexDeclaration decl) {
        String field = SqlLiteral.fieldName(decl.getField().getName());
        String full;
        switch (decl.getTarget().getTargetCase()) {
            case PLAYERS -> full = "pantheon_player_" + SqlLiteral.fieldName(decl.getField().getNamespace()) + "_" + field;
            case STORE -> {
                String store = decl.getTarget().getStore();
                SqlLiteral.store(store); // validates <namespace>:<store>
                full = "pantheon_doc_" + store.replace(':', '_') + "_" + field;
            }
            default -> throw new IllegalArgumentException("index has no target");
        }
        return shorten(full);
    }

    static String shorten(String full) {
        if (full.length() <= MAX) return full;
        try {
            byte[] hash = MessageDigest.getInstance("SHA-256").digest(full.getBytes(StandardCharsets.UTF_8));
            return full.substring(0, KEEP) + "_" + HexFormat.of().formatHex(hash, 0, 4);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException(e);
        }
    }
}
