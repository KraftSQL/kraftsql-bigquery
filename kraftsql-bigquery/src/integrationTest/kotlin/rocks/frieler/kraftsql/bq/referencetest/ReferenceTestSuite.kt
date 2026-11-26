package rocks.frieler.kraftsql.bq.referencetest

import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.DatasetId
import com.google.cloud.bigquery.DatasetInfo
import org.junit.platform.suite.api.AfterSuite
import org.junit.platform.suite.api.BeforeSuite
import org.junit.platform.suite.api.SelectPackages
import org.junit.platform.suite.api.Suite
import rocks.frieler.kraftsql.bq.engine.BigQueryClientFactory
import rocks.frieler.kraftsql.referencetest.ReferenceTestConfiguration
import kotlin.time.Duration.Companion.days

/**
 * Runs KraftSQL's reference test suite against BigQuery.
 *
 * BigQuery requires the dataset of a table to exist, so the suite creates the dataset named by
 * [ReferenceTestConfiguration.namespace][rocks.frieler.kraftsql.referencetest.ReferenceTestConfiguration.namespace] in
 * the [project][ReferenceTestConfiguration.database] before and deletes it after all tests.
 */
@Suite
@SelectPackages("rocks.frieler.kraftsql.referencetest")
class ReferenceTestSuite {
    companion object {
        private val bqClient = BigQueryClientFactory.clientFromEnvironment()
        private val testDatasetId = DatasetId.of(ReferenceTestConfiguration.database, ReferenceTestConfiguration.namespace)

        /**
         * Creates the dataset for the reference tests in the default project, in the location configured for the
         * [BigQueryEngine][rocks.frieler.kraftsql.bq.engine.BigQueryEngine], if any.
         *
         * Tables in the dataset expire after one day by default, to limit the costs of leftovers, when the dataset could
         * not be deleted, e.g. because the test run was aborted.
         */
        /* Design decision: Don't use the dataset, if it already exists, to ensure test-isolation. */
        @BeforeSuite
        @JvmStatic
        fun createDataset() {
            bqClient.create(
                DatasetInfo.newBuilder(testDatasetId)
                    // The client does not apply its configured location to new datasets by itself:
                    .apply { bqClient.options.location?.also { setLocation(it) } }
                    .setDefaultTableLifetime(1.days.inWholeMilliseconds)
                    .build()
            )
        }

        /**
         * Deletes the dataset of the reference tests including all its tables, also those left over by failed tests.
         */
        @AfterSuite
        @JvmStatic
        fun deleteDataset() {
            bqClient.delete(testDatasetId, BigQuery.DatasetDeleteOption.deleteContents())
        }
    }
}
