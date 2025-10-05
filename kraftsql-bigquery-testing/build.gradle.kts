project.description = "KrafSQL BigQuery Testing Support"

plugins {
    id(libs.plugins.kotlin.jvm.get().pluginId)
    `java-library`
    alias(libs.plugins.dokka.javadoc)
    alias(libs.plugins.kover)
    id("kraftsql-publishing")
}

dependencies {
    api(libs.kraftsql.core.testing)
    implementation(libs.junit5.api)
    implementation(project(":kraftsql-bigquery"))
    implementation(libs.jsonpath) {
        runtimeOnly(libs.slf4j.nop)
    }
    implementation(libs.apache.commons.csv)

    testImplementation(libs.kotlin.test.junit5)
    testImplementation(libs.kotest.assertions.core)
    testImplementation(libs.mockito)
    testRuntimeOnly(libs.junit5.engine)
    testRuntimeOnly(libs.junit.platform.launcher)
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(21)
    }
}

tasks.test {
    useJUnitPlatform()
}
