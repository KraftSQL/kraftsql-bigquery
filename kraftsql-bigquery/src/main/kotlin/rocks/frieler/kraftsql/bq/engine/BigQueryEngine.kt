package rocks.frieler.kraftsql.bq.engine

import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.FieldValue
import com.google.cloud.bigquery.QueryJobConfiguration
import rocks.frieler.kraftsql.data.Data
import rocks.frieler.kraftsql.data.DataRow
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
     * @param data the [Data] to collect
     * @param type the Kotlin type of the data
     * @return the data as a list of Kotlin objects
     */
    override fun <T : Any> collect(data: Data<T>, type: KClass<T>): List<T> {
        val result = bigquery.query(QueryJobConfiguration.of(sqlGenerator.makeSQL(data)))

        return result.values.map { valueList -> DataRow(data.schema.map { column -> column.name to valueList[column.name].value(column.type) }) }
            .map { @Suppress("UNCHECKED_CAST") if (type == DataRow::class) it as T else DataClassSerDe.deserialize(it, type) }
    }

    private fun FieldValue.value(type: Type) = when {
        type.nullable && isNull -> null
        type in setOf(Type.INTEGER, Type.NULLABLE_INTEGER) -> longValue.toInt()
        type in setOf(Type.STRING, Type.NULLABLE_STRING) -> stringValue
        else -> throw NotImplementedError("Collecting $type is not implemented.")
    }
}
