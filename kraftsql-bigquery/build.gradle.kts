plugins {
    alias(libs.plugins.kotlin.jvm)
    `java-library`
}

dependencies {
    implementation(libs.kraftsql.core)
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}
