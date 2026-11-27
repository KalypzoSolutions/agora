plugins {
    `java-library`
    alias(libs.plugins.protobuf)
    alias(libs.plugins.maven.publish)
}

java {
    toolchain.languageVersion = JavaLanguageVersion.of(25)
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

// Maven Central: -SNAPSHOT versions go to the Central snapshot repository, others are released automatically.
mavenPublishing {
    publishToMavenCentral(automaticRelease = true)
    // Signing needs signingInMemoryKey (~/.gradle/gradle.properties or ORG_GRADLE_PROJECT_* in CI);
    // without it publishToMavenLocal still works, unsigned.
    if (providers.gradleProperty("signingInMemoryKey").orNull?.isNotBlank() == true) {
        signAllPublications()
    }
    coordinates("de.kalypzo", "agora-proto", version.toString())
    pom {
        name = "agora-proto"
        description = "Agora gRPC protocol: proto definitions and generated Java stubs."
        inceptionYear = "2026"
        url = "https://github.com/KalypzoSolutions/agora"
        licenses {
            license {
                name = "MIT License"
                url = "https://opensource.org/licenses/MIT"
                distribution = "repo"
            }
        }
        developers {
            developer {
                id = "kalypzo"
                name = "Kalypzo"
                url = "https://github.com/KalypzoSolutions"
            }
        }
        scm {
            url = "https://github.com/KalypzoSolutions/agora"
            connection = "scm:git:https://github.com/KalypzoSolutions/agora.git"
            developerConnection = "scm:git:ssh://git@github.com/KalypzoSolutions/agora.git"
        }
    }
}

// Generated gRPC/protobuf sources: their Javadoc is not ours to fix.
tasks.javadoc {
    (options as StandardJavadocDocletOptions).addStringOption("Xdoclint:none", "-quiet")
}
