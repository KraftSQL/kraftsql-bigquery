package rocks.frieler.kraftsql.bq.expressions

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.objects.Data
import rocks.frieler.kraftsql.expressions.Expression
import rocks.frieler.kraftsql.objects.HasColumns

/**
 * BigQuery's [UNNEST](https://docs.cloud.google.com/bigquery/docs/reference/standard-sql/query-syntax#unnest_operator)
 * operator that expands an array [Expression] into rows.
 *
 * @param T the Kotlin type of the array's elements
 * @param arrayExpression the array [Expression] to expand
 */
class Unnest<T : Any>(
    val arrayExpression: Expression<BigQueryEngine, Array<out T>?>,
) : Expression<BigQueryEngine, Data<T>>, HasColumns<BigQueryEngine, T> {
    override fun defaultColumnName() = "UNNEST(${arrayExpression.defaultColumnName()})"

    override val columnNames = listOf("")

    override fun sql() = "UNNEST(${arrayExpression.sql()})"

    override fun equals(other: Any?) = other is Unnest<*>
            && other.arrayExpression == arrayExpression

    override fun hashCode() = arrayExpression.hashCode()
}
