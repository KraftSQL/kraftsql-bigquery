allprojects {
    group = "rocks.frieler.kraftsql"
    version = "0.0.1-SNAPSHOT"

    repositories {
        mavenCentral()
        if (version.toString().endsWith("-SNAPSHOT")) {
            mavenLocal()
            maven("https://maven.pkg.github.com/KraftSQL/-") {
                credentials { // Although the packages are public, GitHub's maven registry requires authentication:
                    username = findProperty("github_packages_user")?.toString()
                    password = findProperty("github_packages_password")?.toString()
                }
            }
        }
    }

    configurations.all {
        resolutionStrategy {
            cacheChangingModulesFor(0, TimeUnit.SECONDS)
        }
    }
}
