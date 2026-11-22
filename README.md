# KraftSQL's Google BigQuery Connector

[![Quality Gate Status](https://sonarcloud.io/api/project_badges/measure?project=kraftsql_kraftsql-bigquery&metric=alert_status)](https://sonarcloud.io/project/overview?id=kraftsql_kraftsql-bigquery)

KraftSQL Connector for [Google BigQuery](https://cloud.google.com/bigquery).

## Usage

Add `rocks.frieler.kraftsql:kraftsql-bigquery` as dependency to your Kotlin application.
It provides the [`BigQueryEngine`](kraftsql-bigquery/src/main/kotlin/rocks/frieler/kraftsql/bq/engine/BigQueryEngine.kt), which documents its configuration.

## Development

### KraftSQL Core dependency

During development the connector will often depend on the current SNAPSHOT version of the core library, not available at Maven Central, or even your local changes.
- To obtain SNAPSHOT dependencies from GitHub's Maven registry configure the Gradle properties `github_packages_user` and `github_packages_password` (a token with `read:packages` scope).
- To build against a local checkout of the core library set the Gradle property `kraftsql.core.localPath` to its path.

### Integration tests

The `integrationTest`s run **Kraft**SQL's reference test suite against the real BigQuery service.

They need access to BigQuery, configured from the environment as described for the `BigQueryEngine`, and a dataset as described for `ReferenceTestConfiguration`.
To isolate test runs, the suite creates the dataset before the tests and deletes it with all its tables afterwards, so ensure a unique name and the necessary permissions.
