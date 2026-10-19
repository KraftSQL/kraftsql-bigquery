package rocks.frieler.kraftsql.bq.engine

import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryOptions

/**
 * Factory to help obtain a [BigQuery] API client.
 */
object BigQueryClientFactory {
    /**
     * Creates a [BigQuery] API client configured from the environment:
     * - credentials: [Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials)
     * - project: the default project of the Google Cloud client libraries, e.g. from the environment variable
     *   `GOOGLE_CLOUD_PROJECT` or the Application Default Credentials
     * - location (optional): the [location](https://cloud.google.com/about/locations) where queries are executed,
     *   from the environment variable `KRAFTSQL_BIGQUERY_LOCATION`; if not set, BigQuery's default location applies
     */
    fun clientFromEnvironment(): BigQuery {
        val bqOptions = BigQueryOptions.newBuilder()
            .apply { System.getenv("KRAFTSQL_BIGQUERY_LOCATION")?.also { setLocation(it) } }
            .build()
        return bqOptions.service
    }
}
