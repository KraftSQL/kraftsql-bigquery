package rocks.frieler.kraftsql.bq.expressions

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.expressions.ArrayConcatenation
import rocks.frieler.kraftsql.expressions.Expression

/**
 * BigQuery's
 * [ARRAY_CONCAT](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/array_functions#array_concat)
 * function.
 *
 * @param T the Kotlin type of the array's elements
 * @param arrays the arrays to concatenate
 */
class ArrayConcat<T>(
    vararg arrays: Expression<BigQueryEngine, Array<T>?>
) : ArrayConcatenation<BigQueryEngine, T>(arrays.toList().toTypedArray()) {
    override val subexpressions = arrays.toList()

    override fun sql() = "ARRAY_CONCAT(${arguments.joinToString(",") { it.sql() }})"

    override fun defaultColumnName() =
        throw IllegalStateException("BigQuery does not provide a predictable default column name based on the evaluated expression.")
}
