plugins {
    id("com.vanniktech.maven.publish")
}

mavenPublishing {
    pom {
        name = project.name
        description = project.description
        url = "https://github.com/KraftSQL"
        licenses {
            license {
                name = "MIT"
                url = "https://opensource.org/licenses/MIT"
            }
        }
        scm {
            url = "https://github.com/KraftSQL/kraftsql-bigquery"
        }
        developers {
            developer { name = "Christopher Frieler" }
        }
    }
    publishToMavenCentral(automaticRelease = true)
    signAllPublications()
}

publishing {
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
