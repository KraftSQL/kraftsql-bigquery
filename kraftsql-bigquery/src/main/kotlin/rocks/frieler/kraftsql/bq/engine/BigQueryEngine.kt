package rocks.frieler.kraftsql.bq.engine

import com.google.cloud.bigquery.BigQueryOptions
import rocks.frieler.kraftsql.bq.engine.api.ApiClientBigQueryConnection
import rocks.frieler.kraftsql.engine.Engine

/**
 * The BigQuery [Engine].
 */
object BigQueryEngine : Engine<BigQueryEngine> {

    object DefaultConnection : rocks.frieler.kraftsql.engine.DefaultConnection<BigQueryEngine, BigQueryConnection>() {
        override fun instantiate(): BigQueryConnection {
            val bigQueryOptions = BigQueryOptions.newBuilder()
                .apply { System.getenv("KRAFTSQL_BIGQUERY_LOCATION")?.also { setLocation(it) } }
                .build()
            return ApiClientBigQueryConnection(bigQueryOptions.service)
                .apply { System.getenv("KRAFTSQL_BIGQUERY_DEFAULT_SESSION_MODE")?.also { setSessionMode(it.toBoolean()) }
            }
        }
    }
}
