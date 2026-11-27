# Agora

gRPC-Microservice, mit dem Pantheon Spielerdaten, Presence und Nachrichten austauscht.

Module: `agora-proto` (Proto und generierte Stubs), `agora-server` (Server).

```
./gradlew build publishToMavenLocal
```

## Betrieb

### Start

```
docker run -p 9090:9090 -v ./tokens:/etc/agora/tokens:ro \
  -e AGORA_DB_URL=jdbc:postgresql://db:5432/agora -e AGORA_DB_USER=... -e AGORA_DB_PASSWORD=... \
  -e AGORA_TOKENS_FILE=/etc/agora/tokens agora:0.1.0-SNAPSHOT
```

Image bauen: `./gradlew :agora-server:jibDockerBuild` (Basis `eclipse-temurin:25-jre`, Image `agora:0.1.0-SNAPSHOT`).
Ohne Docker: `./gradlew :agora-server:run` mit denselben Variablen. PostgreSQL 17; Migrationen laufen beim Start.

### Variablen

| Variable | Default | Bedeutung |
|---|---|---|
| `AGORA_DB_URL` | Pflicht | JDBC-URL |
| `AGORA_DB_USER` / `AGORA_DB_PASSWORD` | Pflicht | Zugang |
| `AGORA_TOKENS_FILE` | Pflicht | Tokens-Datei (siehe unten) |
| `AGORA_PORT` | `9090` | gRPC-Port (Plaintext) |
| `AGORA_DB_POOL_SIZE` | `16` | Hikari-Pool |
| `AGORA_PAPER_WELCOME_DELAY_MS` | `15000` | frühestens so lange nach dem Start bekommen PAPER-Nodes `Welcome` |

### Tokens-Datei

Eine Zeile `node_id=token`, `#` beginnt einen Kommentar. Nodes senden `x-agora-node: <node_id>` und
`authorization: Bearer <token>`. Der Health-Service (`grpc.health.v1`) braucht keine Authentifizierung.

### Shutdown

SIGTERM/SIGINT: Health `NOT_SERVING`, alle Node-Streams enden mit `UNAVAILABLE` (die Nodes verbinden sich neu, ohne
Presence-Events oder node-gone), `server.shutdown()`, höchstens 10 s Warten, danach Hikari schließen.

### Programmatischer Einstieg (Tests, z. B. Pantheon P-11)

```java
AgoraServer server = AgoraServer.start(new AgoraConfig(
        0, jdbcUrl, user, password, 4, tokensFile, Duration.ZERO));   // port 0 = zufälliger Port
int port = server.port();
// ... rohe gRPC-Clients oder Pantheon gegen localhost:port ...
server.close();                                                         // idempotent
```

`AgoraServer.start(AgoraConfig)` verdrahtet alles: Daten, Indizes, Holder/DocChanged-Routing, Presence, Delivery,
Locate und die Hintergrund-Jobs. Die Überladungen mit mehr Argumenten sind Bausteine für Tests einzelner Komponenten
und verdrahten Delivery/Locate nicht. Ein Neustart auf demselben Port (`new AgoraConfig(port, ...)`) ist möglich.
