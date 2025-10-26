package rocks.frieler.kraftsql.bq.expressions

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.expressions.Expression
import java.time.Instant

/**
 * BigQuery's [`TIMESTAMP`](https://cloud.google.com/bigquery/docs/reference/standard-sql/timestamp_functions#timestamp)
 * function.
 *
 * @param stringExpression the expression to convert to a BigQuery
 * [TIMESTAMP][rocks.frieler.kraftsql.bq.engine.Types.TIMESTAMP]
 */
class Timestamp(
    val stringExpression: Expression<BigQueryEngine, String?>,
) : Expression<BigQueryEngine, Instant?> {
    override fun sql(): String =
        "TIMESTAMP(${stringExpression.sql()})"

    override fun defaultColumnName(): String =
        "TIMESTAMP(${stringExpression.defaultColumnName()})"

    override fun equals(other: Any?) =
        other is Timestamp && other.stringExpression == stringExpression

    override fun hashCode() = stringExpression.hashCode()
}
