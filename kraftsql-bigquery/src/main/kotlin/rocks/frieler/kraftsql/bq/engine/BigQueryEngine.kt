package rocks.frieler.kraftsql.bq.engine

import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.Field
import com.google.cloud.bigquery.FieldValue
import com.google.cloud.bigquery.QueryJobConfiguration
import com.google.cloud.bigquery.Schema
import com.google.cloud.bigquery.StandardTableDefinition
import com.google.cloud.bigquery.TableId
import com.google.cloud.bigquery.TableInfo
import rocks.frieler.kraftsql.data.Data
import rocks.frieler.kraftsql.data.DataRow
import rocks.frieler.kraftsql.data.Table
import rocks.frieler.kraftsql.data.Type
import rocks.frieler.kraftsql.engine.DataClassSerDe
import rocks.frieler.kraftsql.engine.Engine
import kotlin.reflect.KClass

/**
 * [Engine] implementation for [Google BigQuery](https://cloud.google.com/bigquery).
 *
 * KraftSQL's [Type]s are mapped to BigQuery's native types as defined by [Type.native][native].
 *
 * @constructor Creates a [BigQueryEngine] that executes all queries with the given [BigQuery] client.
 * @param bigquery the [BigQuery] client to execute queries with; defaults to autoconfiguration by [BigQueryClientFactory.clientFromEnvironment]
 * @param sqlGenerator the [BigQuerySqlGenerator] to generate SQL; only extracted and configurable for testing
 */
class BigQueryEngine(
    private val bigquery: BigQuery = BigQueryClientFactory.clientFromEnvironment(),
    private val sqlGenerator: BigQuerySqlGenerator = BigQuerySqlGenerator,
) : Engine {

    /**
     * Collects [Data] by querying it from BigQuery.
     *
     * The [Type]s of the result are checked against the [schema][Data.schema] apart from nullability, which is checked
     * by the actual values, as defined for [Table][rocks.frieler.kraftsql.data.Table]s.
     *
     * @param data the [Data] to collect
     * @param type the Kotlin type of the data
     * @return the data as a list of Kotlin objects
     */
    override fun <T : Any> collect(data: Data<T>, type: KClass<T>): List<T> {
        val result = bigquery.query(QueryJobConfiguration.of(sqlGenerator.makeSQL(data)))

        return result.values.map { valueList -> DataRow(data.schema.map { column -> column.name to valueList[column.name].value(column.type) }) }
            .map { @Suppress("UNCHECKED_CAST") if (type == DataRow::class) it as T else DataClassSerDe.deserialize(it, type) }
    }

    /**
     * Creates the [Table] in BigQuery.
     *
     * The columns get the [native] types of their [Type]s; non-nullable columns get the mode `REQUIRED`, nullable ones
     * `NULLABLE`.
     *
     * @param table the [Table] to create
     * @throws com.google.cloud.bigquery.BigQueryException if the table already exists or the dataset does not exist
     */
    /* Design decision: Tables are created and dropped via the BigQuery API (`BigQuery.create()` and
     * `BigQuery.delete()`), not by DDL statements, like in the former implementation. This does not run a query job. */
    override fun createTable(table: Table<*>) {
        bigquery.create(TableInfo.of(
            table.bqTableId,
            StandardTableDefinition.of(
                Schema.of(table.schema.map { column ->
                    Field.newBuilder(column.name, column.type.native)
                        .setMode(if (column.type.nullable) Field.Mode.NULLABLE else Field.Mode.REQUIRED)
                        .build()
                })
            ),
        ))
    }

    /**
     * Drops the [Table][rocks.frieler.kraftsql.data.Table] from BigQuery.
     *
     * @param table the [Table] to drop
     * @param ifExists whether to silently do nothing if the table does not exist
     * @throws RuntimeException if the table does not exist and [ifExists] is not set
     */
    /* Design decision: See createTable(). The BigQuery API reports a missing table by its result, not by an
     * exception, so the BigQueryEngine throws itself, if [ifExists] is not set. */
    override fun dropTable(table: Table<*>, ifExists: Boolean) {
        val didDelete = bigquery.delete(table.bqTableId)
        check(didDelete || ifExists) { "Table '$table' not found." }
    }

    private val Table<*>.bqTableId : TableId
        get() = if (database!= null) TableId.of(database, namespace, name)
            else TableId.of(namespace, name)

    /**
     * Inserts the [Data] into the [Table][rocks.frieler.kraftsql.data.Table] in BigQuery.
     *
     * @param table the [Table] to insert into
     * @param data the [Data] to insert
     * @return the number of inserted rows, as reported by BigQuery for the DML statement
     * @throws com.google.cloud.bigquery.BigQueryException if the table does not exist or does not fit the values
     */
    override fun <T : Any> insertInto(table: Table<T>, data: Data<T>): Long {
        return bigquery.query(QueryJobConfiguration.of(sqlGenerator.makeInsertIntoSQL(table, data))).totalRows
    }

    private fun FieldValue.value(type: Type) = when {
        type.nullable && isNull -> null
        type in setOf(Type.INTEGER, Type.NULLABLE_INTEGER) -> longValue.toInt()
        type in setOf(Type.STRING, Type.NULLABLE_STRING) -> stringValue
        else -> throw NotImplementedError("Collecting $type is not implemented.")
    }
}
