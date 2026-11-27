package de.kalypzo.agora.proto;

import static org.assertj.core.api.Assertions.assertThat;

import com.google.protobuf.Descriptors.Descriptor;
import com.google.protobuf.Descriptors.EnumDescriptor;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Descriptors.FileDescriptor;
import com.google.protobuf.ByteString;
import de.kalypzo.agora.proto.v1.AgoraOuterClass;
import de.kalypzo.agora.proto.v1.AuditEntry;
import de.kalypzo.agora.proto.v1.AuditLocation;
import de.kalypzo.agora.proto.v1.AuditValue;
import de.kalypzo.agora.proto.v1.DocChanged;
import de.kalypzo.agora.proto.v1.PlayerRow;
import de.kalypzo.agora.proto.v1.RowRef;
import de.kalypzo.agora.proto.v1.Value;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class ProtoContractTest {

    @Test
    void valueKeepsLongExact() throws Exception {
        Value value = Value.newBuilder().setLongValue(Long.MAX_VALUE).build();
        Value parsed = Value.parseFrom(value.toByteArray());
        assertThat(parsed.getLongValue()).isEqualTo(Long.MAX_VALUE);
        assertThat(parsed.getKindCase()).isEqualTo(Value.KindCase.LONG_VALUE);
    }

    @Test
    void docChangedRoundTrip() throws Exception {
        DocChanged original = DocChanged.newBuilder()
                .setRef(RowRef.newBuilder().setPlayerRow(
                        PlayerRow.newBuilder().setPlayer("0190a1b2-0000-7000-8000-000000000001").setNamespace("economy")))
                .setVersion(42)
                .setDeleted(true)
                .putData("coins", Value.newBuilder().setLongValue(Long.MIN_VALUE).build())
                .putData("name", Value.newBuilder().setStringValue("Alex").build())
                .setIdempotencyKey("0190a1b2-0000-7000-8000-000000000002")
                .build();
        DocChanged parsed = DocChanged.parseFrom(original.toByteArray());
        assertThat(parsed).isEqualTo(original);
    }

    @Test
    void auditEntryRoundTrip() throws Exception {
        AuditEntry original = AuditEntry.newBuilder()
                .setId(bytes16(1))
                .setPlayer(bytes16(2))
                .setAction("shop:buy")
                .setLocation(AuditLocation.newBuilder().setWorld("world").setX(1.5).setY(64).setZ(-3.25))
                .setTarget(bytes16(3))
                .putWith("item", AuditValue.newBuilder().setS("diamond").build())
                .putWith("amount", AuditValue.newBuilder().setL(Long.MIN_VALUE).build())
                .putWith("price", AuditValue.newBuilder().setD(12.5).build())
                .putWith("gift", AuditValue.newBuilder().setB(true).build())
                .setTrace(bytes16(4))
                .build();
        AuditEntry parsed = AuditEntry.parseFrom(original.toByteArray());
        assertThat(parsed).isEqualTo(original);
        assertThat(parsed.getWithMap()).hasSize(4);
        assertThat(parsed.getWithMap().get("item").getValueCase()).isEqualTo(AuditValue.ValueCase.S);
        assertThat(parsed.getWithMap().get("amount").getValueCase()).isEqualTo(AuditValue.ValueCase.L);
        assertThat(parsed.getWithMap().get("price").getValueCase()).isEqualTo(AuditValue.ValueCase.D);
        assertThat(parsed.getWithMap().get("gift").getValueCase()).isEqualTo(AuditValue.ValueCase.B);
        assertThat(parsed.hasLocation()).isTrue();
        assertThat(parsed.hasTarget()).isTrue();
        assertThat(parsed.hasTrace()).isTrue();
    }

    private static ByteString bytes16(int seed) {
        byte[] b = new byte[16];
        for (int i = 0; i < 16; i++) b[i] = (byte) (seed * 16 + i);
        return ByteString.copyFrom(b);
    }

    @Test
    void fieldNumbersAreFrozen() throws IOException {
        List<String> actual = new ArrayList<>();
        FileDescriptor file = AgoraOuterClass.getDescriptor();
        for (Descriptor message : file.getMessageTypes()) {
            collect(message, actual);
        }
        for (EnumDescriptor e : file.getEnumTypes()) {
            e.getValues().forEach(v -> actual.add(e.getName() + "." + v.getName() + "=" + v.getNumber()));
        }
        actual.sort(String::compareTo);

        List<String> frozen;
        try (InputStream in = getClass().getResourceAsStream("/agora-v1-fields.txt")) {
            assertThat(in).isNotNull();
            frozen = new String(in.readAllBytes(), StandardCharsets.UTF_8).lines()
                    .map(String::strip).filter(l -> !l.isEmpty()).toList();
        }
        assertThat(frozen).isNotEmpty();
        assertThat(actual).containsAll(frozen);
    }

    private static void collect(Descriptor message, List<String> out) {
        for (FieldDescriptor f : message.getFields()) {
            out.add(message.getName() + "." + f.getName() + "=" + f.getNumber());
        }
        for (Descriptor nested : message.getNestedTypes()) {
            if (!nested.getOptions().getMapEntry()) {
                collect(nested, out);
            }
        }
    }
}
