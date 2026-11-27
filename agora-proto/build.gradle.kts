plugins {
    `java-library`
    `maven-publish`
    alias(libs.plugins.protobuf)
}

java {
    toolchain.languageVersion = JavaLanguageVersion.of(25)
    withSourcesJar()
}

dependencies {
    api(libs.grpc.protobuf)
    api(libs.grpc.stub)
    api(libs.protobuf.java)
    compileOnly(libs.tomcat.annotations.api)

    testImplementation(platform(libs.junit.bom))
    testImplementation(libs.junit.jupiter)
    testImplementation(libs.assertj.core)
    testRuntimeOnly(libs.junit.platform.launcher)
}

tasks.test { useJUnitPlatform() }

protobuf {
    protoc { artifact = "com.google.protobuf:protoc:${libs.versions.protobuf.asProvider().get()}" }
    plugins {
        create("grpc") { artifact = "io.grpc:protoc-gen-grpc-java:${libs.versions.grpc.get()}" }
    }
    generateProtoTasks {
        all().configureEach { plugins { create("grpc") } }
    }
}

publishing {
    publications {
        create<MavenPublication>("maven") { from(components["java"]) }
    }
}
