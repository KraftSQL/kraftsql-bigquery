plugins {
    `maven-publish`
}

publishing {
    publications {
        create<MavenPublication>("maven") {
            from(components["java"])
            artifact(tasks["kotlinSourcesJar"])
        }
    }
    repositories {
        maven {
            name = "GitHubPackages"
            url = uri("https://maven.pkg.github.com/KraftSQL/kraftsql-bigquery")
            credentials {
                username = findProperty("github_packages_publishing_user")?.toString()
                password = findProperty("github_packages_publishing_password")?.toString()
            }
        }
    }
}
