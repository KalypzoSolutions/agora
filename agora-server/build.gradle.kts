plugins {
    `java-library`
    application
    `maven-publish`
    alias(libs.plugins.jib)
}

java {
    toolchain.languageVersion = JavaLanguageVersion.of(25)
    withSourcesJar()
}

application {
    mainClass = "de.kalypzo.agora.Main"
}

dependencies {
    api(project(":agora-proto"))
    implementation(libs.grpc.netty.shaded)
    implementation(libs.grpc.services)
    implementation(libs.grpc.inprocess)
    implementation(libs.protobuf.java.util)
    implementation(libs.hikaricp)
    implementation(libs.flyway.core)
    implementation(libs.flyway.database.postgresql)
    implementation(libs.postgresql)
    implementation(libs.slf4j.api)
    runtimeOnly(libs.logback.classic)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testRuntimeOnly(libs.junit.platform.launcher)
    testImplementation(libs.assertj.core)
    testImplementation(platform(libs.testcontainers.bom))
    testImplementation(libs.testcontainers.postgresql)
    testImplementation(libs.testcontainers.junit.jupiter)
    testImplementation(libs.awaitility)
    testImplementation(libs.logback.classic)
}

tasks.test {
    useJUnitPlatform()
}

// Not on Maven Central (it ships as a Docker image); publishToMavenLocal serves Pantheon's integration tests and CI.
publishing {
    publications {
        create<MavenPublication>("maven") { from(components["java"]) }
    }
}

jib {
    from { image = "eclipse-temurin:25-jre" }
    to { image = "agora:${project.version}" }
    container {
        mainClass = "de.kalypzo.agora.Main"
        ports = listOf("9090")
        creationTime = "USE_CURRENT_TIMESTAMP"
        jvmFlags = listOf("--enable-native-access=ALL-UNNAMED", "-XX:MaxRAMPercentage=75")
    }
}
