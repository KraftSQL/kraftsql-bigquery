allprojects {
    group = "rocks.frieler.kraftsql"
    version = "0.0.14-SNAPSHOT".also {
        afterEvaluate {
            val kraftsqlDependencyVersion = libs.versions.kraftsql.get()
            check(it == kraftsqlDependencyVersion) { "Connector version $it must match global KraftSQL version $kraftsqlDependencyVersion." }
        }
    }

    repositories {
        mavenCentral()
        maven("https://maven.pkg.github.com/KraftSQL/-") {
            credentials { // Although the packages are public, GitHub's maven registry requires authentication:
                username = findProperty("github_packages_user")?.toString()
                password = findProperty("github_packages_password")?.toString()
            }
            mavenContent {
                snapshotsOnly()
                includeGroup("rocks.frieler.kraftsql")
            }
        }
    }

    configurations.all {
        resolutionStrategy {
            cacheChangingModulesFor(0, TimeUnit.SECONDS)
        }
    }
}

plugins {
}
