# Agora

gRPC-Microservice, mit dem Pantheon Spielerdaten, Presence und Nachrichten austauscht.

Module: `agora-proto` (Proto und generierte Stubs), `agora-server` (Server).

```
./gradlew build publishToMavenLocal
```

## Veröffentlichung

`agora-proto` liegt auf Maven Central als `de.kalypzo:agora-proto`. `agora-server` kommt nicht nach Maven Central,
sondern als Image nach `ghcr.io/kalypzosolutions/agora`; `publishToMavenLocal` stellt ihn für Pantheons
Integrationstests bereit.

- **Release:** Tag `v<version>` pushen (z. B. `git tag v0.1.0 && git push origin v0.1.0`). Der Workflow
  `release.yml` baut mit `-Pversion=<version>`, gibt automatisch auf Central frei und pusht das Image mit den Tags
  `<version>` und `latest`.
- **Snapshot:** jeder Push auf `main` veröffentlicht die `-SNAPSHOT`-Version aus `gradle.properties` nach
  `https://central.sonatype.com/repository/maven-snapshots/` und pusht das Image mit den Tags `<version>-SNAPSHOT`
  und `sha-<commit>`.
- Nach einem Release die Version in `gradle.properties` auf die nächste `-SNAPSHOT` setzen.
- Lokal: `./gradlew publishToMavenCentral` mit Zugangsdaten und Signing-Key in `~/.gradle/gradle.properties`.
- GitHub-Secrets: `MAVEN_CENTRAL_USERNAME`, `MAVEN_CENTRAL_PASSWORD`, `SIGNING_KEY`, `SIGNING_KEY_ID`,
  `SIGNING_KEY_PASSWORD`.

## Betrieb

### Start

```
docker run -p 9090:9090 -v ./tokens:/etc/agora/tokens:ro \
  -e AGORA_DB_URL=jdbc:postgresql://db:5432/agora -e AGORA_DB_USER=... -e AGORA_DB_PASSWORD=... \
  -e AGORA_TOKENS_FILE=/etc/agora/tokens ghcr.io/kalypzosolutions/agora:<version>
```

Image bauen: `./gradlew :agora-server:jibDockerBuild` (Basis `eclipse-temurin:25-jre`, Image `agora:<version>`).
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
| `AGORA_TELEPORT_PREPARE_MS` | `3000` | längste Wartezeit auf `ArrivalPrepared` beim Teleport |
| `AGORA_TELEPORT_CONNECT_MS` | `10000` | längste Wartezeit auf `ConnectResult` beim Teleport |
| `AGORA_TELEPORT_ARRIVAL_MS` | `25000` | längste Wartezeit auf `PlayerArrived` beim Teleport |
| `AGORA_TELEPORT_TOTAL_MS` | `38000` | längste Gesamtdauer eines Teleports |

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
