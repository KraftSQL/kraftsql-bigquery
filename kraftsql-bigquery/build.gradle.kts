project.description = "KrafSQL BigQuery Connector"

plugins {
    id(libs.plugins.kotlin.jvm.get().pluginId)
    `java-library`
    id("test-jvm-agents")
    alias(libs.plugins.dokka.javadoc)
    alias(libs.plugins.kover)
    id("kraftsql-publishing")
}

dependencies {
    api(libs.kraftsql.core)
    implementation(platform(libs.google.cloud.libraries.bom))
    implementation(libs.bigquery.client)

    testImplementation(libs.kotlin.test.junit5)
    testImplementation(libs.kotest.assertions.core)
    testImplementation(libs.mockito)
    testAgent(libs.mockito.core) { isTransitive = false }
    testRuntimeOnly(libs.junit5.engine)
    testRuntimeOnly(libs.junit.platform.launcher)
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
}

tasks.test {
    useJUnitPlatform()
}
