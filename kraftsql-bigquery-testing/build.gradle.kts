project.description = "KrafSQL BigQuery Testing Support"

plugins {
    id(libs.plugins.kotlin.jvm.get().pluginId)
    `java-library`
    alias(libs.plugins.dokka.javadoc)
    id("kraftsql-publishing")
}

dependencies {
    api(libs.kraftsql.core.testing)
    implementation(libs.junit5.api)
    implementation(project(":kraftsql-bigquery"))
    implementation(libs.jsonpath) {
        runtimeOnly(libs.slf4j.nop)
    }
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}
