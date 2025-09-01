package rocks.frieler.kraftsql.bq.engine

import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigquery.BigQuerySQLException
import com.google.cloud.bigquery.ConnectionProperty
import com.google.cloud.bigquery.Field
import com.google.cloud.bigquery.FieldList
import com.google.cloud.bigquery.JobStatistics
import com.google.cloud.bigquery.QueryJobConfiguration
import com.google.cloud.bigquery.Schema
import com.google.cloud.bigquery.StandardTableDefinition
import com.google.cloud.bigquery.TableId
import com.google.cloud.bigquery.TableInfo
import rocks.frieler.kraftsql.bq.objects.Table
import rocks.frieler.kraftsql.ddl.CreateTable
import rocks.frieler.kraftsql.ddl.DropTable
import rocks.frieler.kraftsql.dml.BeginTransaction
import rocks.frieler.kraftsql.dml.CommitTransaction
import rocks.frieler.kraftsql.dml.Delete
import rocks.frieler.kraftsql.dml.InsertInto
import rocks.frieler.kraftsql.dml.RollbackTransaction
import rocks.frieler.kraftsql.engine.Connection
import rocks.frieler.kraftsql.engine.DefaultConnection
import rocks.frieler.kraftsql.dql.Select
import kotlin.reflect.KClass

class BigQueryConnection(
    private val bigquery: BigQuery,
) : Connection<BigQueryEngine> {
    private var activeSession: String? = null

    override fun <T : Any> execute(select: Select<BigQueryEngine, T>, type: KClass<T>): List<T> {
        val jobConfiguration = QueryJobConfiguration
            .newBuilder(select.sql())
            .setUseLegacySql(false)
            .apply { if (activeSession != null) setConnectionProperties(listOf(ConnectionProperty.of("session_id", activeSession))) }
            .build()
        val result = bigquery.query(jobConfiguration)
        return BigQueryORMapping.deserializeQueryResult(
            result.iterateAll()
                .map { fieldValues ->
                    result.schema!!.fields.associateWith { field ->
                        fieldValues.get(field.name)
                    }
                },
            type)
    }

    override fun execute(createTable: CreateTable<BigQueryEngine>) {
        val table = createTable.table as Table<*>

        val tableId = if (table.project != null) {
            TableId.of(table.project, table.dataset, table.name)
        } else {
            TableId.of(table.dataset, table.name)
        }

        fun constructField(name: String, type: Type): Field =
            when (type) {
                // TODO: nullability
                is Types.ARRAY -> Field.newBuilder(name, type.contentType.name).setMode(Field.Mode.REPEATED)
                    .build()
                is Types.STRUCT -> Field.newBuilder(
                    name,
                    type.name,
                    FieldList.of(type.fields.map { (subfieldName, subfieldType) -> constructField(subfieldName, subfieldType) })
                ).build()
                else -> Field.of(name, type.name)
            }
        val schema = Schema.of(table.columns.map { column -> constructField(column.name, column.type as Type) })

        val tableInfo = TableInfo.of(tableId, StandardTableDefinition.of(schema))
        bigquery.create(tableInfo)
    }

    override fun execute(dropTable: DropTable<BigQueryEngine>) {
        val didDelete = bigquery.delete((dropTable.table as Table).getTableId())
        if (!didDelete && !dropTable.ifExists) {
            throw BigQuerySQLException("Table '${dropTable.table.qualifiedName}' to drop did not exist")
        }
    }

    override fun execute(insertInto: InsertInto<BigQueryEngine, *>): Int {
        val jobConfiguration = QueryJobConfiguration
            .newBuilder(insertInto.sql())
            .setUseLegacySql(false)
            .apply { if (activeSession != null) setConnectionProperties(listOf(ConnectionProperty.of("session_id", activeSession))) }
            .build()
        val result = bigquery.query(jobConfiguration)
        return result.totalRows.toInt()
    }

    override fun execute(delete: Delete<BigQueryEngine>): Int {
        require(delete is rocks.frieler.kraftsql.bq.dml.Delete) { "BigQuery requires its own Delete implementation." }

        val jobConfiguration = QueryJobConfiguration
            .newBuilder(delete.sql())
            .setUseLegacySql(false)
            .apply { if (activeSession != null) setConnectionProperties(listOf(ConnectionProperty.of("session_id", activeSession))) }
            .build()
        val result = bigquery.query(jobConfiguration)
        return result.totalRows.toInt()
    }

    override fun execute(beginTransaction: BeginTransaction<BigQueryEngine>) {
        val jobConfiguration = QueryJobConfiguration
            .newBuilder(beginTransaction.sql())
            .setUseLegacySql(false)
            .apply {
                if (activeSession != null) {
                    setConnectionProperties(listOf(ConnectionProperty.of("session_id", activeSession)))
                } else {
                    setCreateSession(true)
                }
            }
            .build()
        bigquery.query(jobConfiguration).also {
            activeSession = activeSession ?: bigquery.getJob(it.jobId).getStatistics<JobStatistics.QueryStatistics>().sessionInfo.sessionId
        }
    }

    override fun execute(commitTransaction: CommitTransaction<BigQueryEngine>) {
        val jobConfiguration = QueryJobConfiguration
            .newBuilder(commitTransaction.sql())
            .setUseLegacySql(false)
            .apply { if (activeSession != null) setConnectionProperties(listOf(ConnectionProperty.of("session_id", activeSession))) }
            .build()
        bigquery.query(jobConfiguration)
        // TODO: terminate session?
    }

    override fun execute(rollbackTransaction: RollbackTransaction<BigQueryEngine>) {
        val jobConfiguration = QueryJobConfiguration
            .newBuilder(rollbackTransaction.sql())
            .setUseLegacySql(false)
            .apply { if (activeSession != null) setConnectionProperties(listOf(ConnectionProperty.of("session_id", activeSession))) }
            .build()
        bigquery.query(jobConfiguration)
        // TODO: terminate session?
    }

    private fun Table<*>.getTableId() =
        if (project != null) {
            TableId.of(project, dataset, name)
        } else {
            TableId.of(dataset, name)
        }

    object Default : DefaultConnection<BigQueryEngine>() {
        override fun instantiate(): Connection<BigQueryEngine> {
            return BigQueryConnection(BigQueryOptions.getDefaultInstance().service)
        }
    }
}
