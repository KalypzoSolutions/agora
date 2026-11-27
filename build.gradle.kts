allprojects {
    group = "de.kalypzo"
    // version: gradle.properties; CI release overrides it with -Pversion=<tag without "v">
    repositories {
        mavenCentral()
    }
}
