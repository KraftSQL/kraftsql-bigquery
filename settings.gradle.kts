rootProject.name = "KraftSQL BigQuery"
include("kraftsql-bigquery")

providers.gradleProperty("kraftsql.core.localPath").orNull?.let { includeBuild(it) }
