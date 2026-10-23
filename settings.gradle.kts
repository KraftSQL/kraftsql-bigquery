rootProject.name = "kraftsql-bigquery"
include("kraftsql-bigquery")

providers.gradleProperty("kraftsql.core.localPath").orNull?.let { includeBuild(it) }
