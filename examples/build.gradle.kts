plugins {
    id(libs.plugins.kotlin.jvm.get().pluginId)
    application
}

dependencies {
    implementation(project(":kraftsql-bigquery"))

    testImplementation(libs.kotlin.test.junit5)
    testImplementation(libs.kotest.assertions.core)
    testImplementation(project(":kraftsql-bigquery-testing"))
    testRuntimeOnly(libs.junit5.engine)
    testRuntimeOnly(libs.junit.platform.launcher)
}

java {
    toolchain {
        languageVersion = JavaLanguageVersion.of(24)
    }
}

tasks.test {
    useJUnitPlatform()
}
