allprojects {
    group = "rocks.frieler.kraftsql"
    version = "0.0.2-SNAPSHOT"

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

tasks.register("versionFile") {
    group = "build"
    description = "Writes the current package version to 'build/version.txt'."

    val versionTxt = layout.buildDirectory.get().file("version.txt").asFile
    val version = project.version.toString()
    doLast {
        versionTxt.apply {
            parentFile.mkdirs()
            writeText(version)
        }
    }
}
