package rocks.frieler.kraftsql.bq.engine.api

import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQuerySQLException
import com.google.cloud.bigquery.Field
import com.google.cloud.bigquery.FieldList
import com.google.cloud.bigquery.QueryJobConfiguration
import com.google.cloud.bigquery.Schema
import com.google.cloud.bigquery.StandardTableDefinition
import com.google.cloud.bigquery.TableId
import com.google.cloud.bigquery.TableInfo
import com.google.cloud.bigquery.TableResult
import rocks.frieler.kraftsql.bq.dml.LoadData
import rocks.frieler.kraftsql.bq.engine.BigQueryConnection
import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.engine.BigQueryORMapping
import rocks.frieler.kraftsql.bq.engine.Type
import rocks.frieler.kraftsql.bq.engine.Types
import rocks.frieler.kraftsql.bq.objects.Table
import rocks.frieler.kraftsql.bq.objects.TemporaryTable
import rocks.frieler.kraftsql.ddl.CreateTable
import rocks.frieler.kraftsql.ddl.DropTable
import rocks.frieler.kraftsql.dml.BeginTransaction
import rocks.frieler.kraftsql.dml.CommitTransaction
import rocks.frieler.kraftsql.dml.Delete
import rocks.frieler.kraftsql.dml.InsertInto
import rocks.frieler.kraftsql.dml.RollbackTransaction
import rocks.frieler.kraftsql.dql.Select
import kotlin.reflect.KClass

class ApiClientBigQueryConnection(
    private val bigquery: BigQuery,
) : BigQueryConnection {
    override fun <T : Any> execute(select: Select<BigQueryEngine, T>, type: KClass<T>): List<T> {
        val result = executeQuery(QueryJobConfiguration.newBuilder(select.sql()))
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
        if (createTable.table is TemporaryTable<*>) {
            throw UnsupportedOperationException("The BigQuery API does not support creation of temporary tables.")
        }
        val table = createTable.table as Table<*>

        val tableId = if (table.project != null) {
            TableId.of(table.project, table.dataset, table.name)
        } else {
            TableId.of(table.dataset, table.name)
        }

        fun constructField(name: String, type: Type<*>): Field =
            when (type) {
                // TODO: nullability
                is Types.ARRAY<*> -> Field.newBuilder(name, type.contentType.name).setMode(Field.Mode.REPEATED)
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
        if (dropTable.table is TemporaryTable<*>) {
            throw UnsupportedOperationException("The BigQuery API does not support dropping temporary tables.")
        }
        val didDelete = bigquery.delete((dropTable.table as Table).getTableId())
        if (!didDelete && !dropTable.ifExists) {
            throw BigQuerySQLException("Table '${dropTable.table.qualifiedName}' to drop did not exist")
        }
    }

    override fun execute(insertInto: InsertInto<BigQueryEngine, *>): Int {
        val result = executeQuery(QueryJobConfiguration.newBuilder(insertInto.sql()))
        return result.totalRows.toInt()
    }

    override fun execute(delete: Delete<BigQueryEngine>): Int {
        require(delete is rocks.frieler.kraftsql.bq.dml.Delete) { "BigQuery requires its own Delete implementation." }
        val result = executeQuery(QueryJobConfiguration.newBuilder(delete.sql()))
        return result.totalRows.toInt()
    }

    override fun execute(loadData: LoadData) {
        executeQuery(QueryJobConfiguration.newBuilder(loadData.sql()), requireSession = loadData.table is TemporaryTable)
    }

    override fun execute(beginTransaction: BeginTransaction<BigQueryEngine>) {
        executeQuery(QueryJobConfiguration.newBuilder(beginTransaction.sql()), requireSession = true)
    }

    override fun execute(commitTransaction: CommitTransaction<BigQueryEngine>) {
        executeQuery(QueryJobConfiguration.newBuilder(commitTransaction.sql()), requireSession = true)
    }

    override fun execute(rollbackTransaction: RollbackTransaction<BigQueryEngine>) {
        executeQuery(QueryJobConfiguration.newBuilder(rollbackTransaction.sql()), requireSession = true)
    }

    private fun executeQuery(queryJobConfig: QueryJobConfiguration.Builder, requireSession: Boolean = false): TableResult =
        bigquery.query(
            queryJobConfig
                .setUseLegacySql(false)
                .configureSession(sessionHandler, requireSession)
                .build()
        )
            .also { result ->
                sessionHandler?.memorizeSession(result, bigquery)
            }

    private fun Table<*>.getTableId() =
        if (project != null) {
            TableId.of(project, dataset, name)
        } else {
            TableId.of(dataset, name)
        }

    private var sessionHandler : SessionHandler? = null

    override fun setSessionMode(sessionMode: Boolean) {
        if (sessionMode) {
            sessionHandler = sessionHandler ?: SessionHandler()
        } else {
            sessionHandler?.abortSession(bigquery)
            sessionHandler = null
        }
    }

    private fun QueryJobConfiguration.Builder.configureSession(sessionHandler: SessionHandler?, requireSession: Boolean = false) : QueryJobConfiguration.Builder {
        if (sessionHandler != null) {
            sessionHandler.configureSession(this)
        } else if (requireSession) {
            throw IllegalStateException("Statement would require a session, but session mode is turned off.")
        }

        return this
    }
}
