plugins {
    alias(libs.plugins.kotlin.jvm)
    `java-library`
}

dependencies {
    implementation(libs.kraftsql.core)
    implementation(platform(libs.google.cloud.libraries.bom))
    implementation(libs.bigquery.client)
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}
