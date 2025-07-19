plugins {
    alias(libs.plugins.kotlin.jvm)
    `java-library`
}

dependencies {
    implementation(libs.junit5.api)
    implementation(project(":kraftsql-bigquery"))
    implementation(libs.kraftsql.core.testing)
    implementation(libs.kotest.assertions.api)
    implementation(libs.kotest.assertions.shared)
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}
