# KraftSQL's Google BigQuery Connector

KraftSQL Connector for [Google BigQuery](https://cloud.google.com/bigquery).

## Development

### KraftSQL Core dependency

During development the connector will often depend on the current SNAPSHOT version of the core library, not available at Maven Central, or even your local changes.
- To obtain SNAPSHOT dependencies from GitHub's Maven registry configure the Gradle properties `github_packages_user` and `github_packages_password` (a token with `read:packages` scope).
- To build against a local checkout of the core library set the Gradle property `kraftsql.core.localPath` to its path.
