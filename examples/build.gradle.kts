plugins {
    id(libs.plugins.kotlin.jvm.get().pluginId)
    application
}

dependencies {
    implementation(libs.kraftsql.core)
    implementation(project(":kraftsql-bigquery"))

    testImplementation(libs.junit5.api)
    testImplementation(libs.kraftsql.core.testing)
    testImplementation(project(":kraftsql-bigquery-testing"))
    testImplementation(libs.kotlin.test.junit5)
    testImplementation(libs.kotest.assertions.core)
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
