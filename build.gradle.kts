allprojects {
    group = "rocks.frieler.kraftsql"
    version = "0.0.1-SNAPSHOT"

    repositories {
        mavenCentral()
        if ((version as String).endsWith("-SNAPSHOT")) {
            mavenLocal()
        }
    }
}
