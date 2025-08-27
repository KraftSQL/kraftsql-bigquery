project.description = "KrafSQL BigQuery Connector"

plugins {
    id(libs.plugins.kotlin.jvm.get().pluginId)
    `java-library`
    alias(libs.plugins.dokka.javadoc)
    id("kraftsql-publishing")
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
