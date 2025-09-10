package rocks.frieler.kraftsql.bq.engine.api

import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.ConnectionProperty
import com.google.cloud.bigquery.JobStatistics
import com.google.cloud.bigquery.QueryJobConfiguration
import com.google.cloud.bigquery.TableResult

internal class SessionHandler {
    var activeSession : String? = null

    fun configureSession(jobConfig: QueryJobConfiguration.Builder, ensureSession: Boolean = true) {
        if (activeSession != null) {
            jobConfig.setConnectionProperty(ConnectionProperty.of("session_id", activeSession))
        } else if (ensureSession) {
            jobConfig.setCreateSession(true)
        }
    }

    fun memorizeSession(result: TableResult, bigquery: BigQuery) {
        activeSession = bigquery.getJob(result.jobId).getStatistics<JobStatistics.QueryStatistics>().sessionInfo.sessionId
    }

    fun abortSession(bigquery: BigQuery, force: Boolean = false) {
        if (activeSession != null) {
            bigquery.query(QueryJobConfiguration.newBuilder("CALL BQ.ABORT_SESSION('${activeSession}')").build())
            activeSession = null
        } else if (force) {
            throw IllegalStateException("no active session to abort.")
        }
    }
}
