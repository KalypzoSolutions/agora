package de.kalypzo.agora.delivery;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import de.kalypzo.agora.proto.v1.Deliver;
import de.kalypzo.agora.proto.v1.Delivery;
import de.kalypzo.agora.proto.v1.DeliveryKind;
import de.kalypzo.agora.proto.v1.NodeKind;
import de.kalypzo.agora.proto.v1.NodeMessage;
import de.kalypzo.agora.proto.v1.PresenceEntry;
import de.kalypzo.agora.proto.v1.SendDelivery;
import de.kalypzo.agora.proto.v1.ServiceMessage;
import de.kalypzo.agora.testing.InProcessAgora;
import de.kalypzo.agora.testing.InProcessAgora.Client;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

/** A-12: SendDelivery (E2) is routed to the proxy of the player as Deliver (agora §6). */
class DeliveryRouterTest {

    static final String STEVE = "00000000-0000-0000-0000-000000000001";
    static final String ALEX = "00000000-0000-0000-0000-000000000002";
    static final String BOB = "00000000-0000-0000-0000-000000000003";

    @TempDir Path dir;
    InProcessAgora agora;
    final ListAppender<ILoggingEvent> logs = new ListAppender<>();
    Logger routerLog;
    Level previousLevel;

    @BeforeEach
    void setUp() throws Exception {
        agora = new InProcessAgora(dir, Duration.ofSeconds(3));
        routerLog = (Logger) LoggerFactory.getLogger(DeliveryRouter.class);
        previousLevel = routerLog.getLevel();
        routerLog.setLevel(Level.DEBUG);
        logs.start();
        routerLog.addAppender(logs);
    }

    @AfterEach
    void tearDown() {
        routerLog.detachAppender(logs);
        routerLog.setLevel(previousLevel);
        agora.close();
    }

    @Test
    void deliveryGoesToOwningProxyOnly() throws Exception {
        Client p1 = agora.connect("proxy-01", NodeKind.VELOCITY, entry(STEVE, "Steve", "lobby-01"));
        Client p2 = agora.connect("proxy-02", NodeKind.VELOCITY, entry(ALEX, "Alex", "lobby-02"));
        Client sender = agora.connect("lobby-02", NodeKind.PAPER);

        Delivery hi = chat("hi");
        sender.send(send(STEVE, hi));
        // Marker: sent after Steve's delivery on the same stream, so proxy-02 sees it only after Steve's was routed.
        sender.send(send(ALEX, chat("marker")));

        Deliver atP1 = p1.nextMatching(ServiceMessage::hasDeliver).getDeliver();
        assertThat(atP1).isEqualTo(Deliver.newBuilder().setPlayer(STEVE).setDelivery(hi).build());
        Deliver atP2 = p2.nextMatching(ServiceMessage::hasDeliver).getDeliver();
        assertThat(atP2.getPlayer()).isEqualTo(ALEX); // the first Deliver at proxy-02 is the marker, not Steve's
        assertThat(sender.drained(ServiceMessage::hasDeliver)).isEmpty();
        assertThat(p1.drained(ServiceMessage::hasDeliver)).isEmpty();
    }

    @Test
    void deliveryForOfflinePlayerIsDropped() throws Exception {
        Client p1 = agora.connect("proxy-01", NodeKind.VELOCITY, entry(STEVE, "Steve", "lobby-01"));
        Client sender = agora.connect("lobby-01", NodeKind.PAPER);

        sender.send(send(BOB, chat("lost")));
        sender.send(send(STEVE, chat("kept")));

        Deliver first = p1.nextMatching(ServiceMessage::hasDeliver).getDeliver();
        assertThat(first.getPlayer()).isEqualTo(STEVE);
        assertThat(first.getDelivery().getComponentJson()).contains("kept");
        assertThat(logs.list).anySatisfy(e -> {
            assertThat(e.getLevel()).isEqualTo(Level.DEBUG);
            assertThat(e.getFormattedMessage()).isEqualTo("Zustellung an " + BOB + " verworfen: offline");
        });
    }

    @Test
    void deliveryOrderIsPreserved() throws Exception {
        Client p1 = agora.connect("proxy-01", NodeKind.VELOCITY, entry(STEVE, "Steve", "lobby-01"));
        Client sender = agora.connect("lobby-02", NodeKind.PAPER);

        List<String> sent = new ArrayList<>();
        for (int i = 0; i < 200; i++) {
            String text = "m" + i;
            sent.add(text);
            sender.send(send(STEVE, chat(text)));
        }

        List<String> got = new ArrayList<>();
        for (int i = 0; i < sent.size(); i++) {
            got.add(p1.nextMatching(ServiceMessage::hasDeliver).getDeliver().getDelivery().getComponentJson());
        }
        assertThat(got).containsExactlyElementsOf(sent.stream().map(DeliveryRouterTest::json).toList());
    }

    // ---------- helpers ----------

    static PresenceEntry entry(String player, String name, String server) {
        return PresenceEntry.newBuilder().setPlayer(player).setName(name).setServer(server).setLocale("de_DE").build();
    }

    static String json(String text) {
        return "{\"text\":\"" + text + "\"}";
    }

    static Delivery chat(String text) {
        return Delivery.newBuilder().setKind(DeliveryKind.CHAT).setComponentJson(json(text)).build();
    }

    static NodeMessage send(String player, Delivery d) {
        return NodeMessage.newBuilder().setSendDelivery(SendDelivery.newBuilder().setPlayer(player).setDelivery(d))
                .build();
    }
}
