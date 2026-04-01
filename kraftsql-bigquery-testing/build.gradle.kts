project.description = "KrafSQL BigQuery Testing Support"

plugins {
    id(libs.plugins.kotlin.jvm.get().pluginId)
    `java-library`
    id("test-jvm-agents")
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
    testImplementation(libs.junit.params)
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
kotlin {
    compilerOptions {
        freeCompilerArgs.add("-Xcontext-parameters")
    }
}

tasks.test {
    useJUnitPlatform()
}
