project.description = "KrafSQL BigQuery Connector"

plugins {
    id(libs.plugins.kotlin.jvm.get().pluginId)
    `java-library`
    idea
    id("integration-testing")
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(25)
    }
    tasks.test {
        useJUnitPlatform()
        jvmArgs("-XX:+EnableDynamicAgentLoading")
    }
}

dependencies {
    api(libs.kraftsql.core)
    api(platform(libs.google.cloud.libraries.bom))
    api(libs.bigquery.client)

    testImplementation(libs.junit.api)
    testImplementation(libs.kotest.assertions.core)
    testImplementation(libs.mockk)
    testRuntimeOnly(libs.junit.engine)
    testRuntimeOnly(libs.junit.platform.launcher)

    "integrationTestImplementation"(libs.junit.platform.suite)
    "integrationTestRuntimeOnly"(project(":kraftsql-bigquery"))
    "integrationTestRuntimeOnly"(libs.kraftsql.core) {
        capabilities { requireCapability("${group}:kraftsql-reference-test:${version}") }
    }
}
