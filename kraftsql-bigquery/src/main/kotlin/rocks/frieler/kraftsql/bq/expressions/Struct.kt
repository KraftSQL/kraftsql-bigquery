package rocks.frieler.kraftsql.bq.expressions

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.expressions.Expression
import rocks.frieler.kraftsql.expressions.Row

/**
 * BigQuery's
 * [`STRUCT()`](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#constructing_a_struct)
 * [Expression].
 *
 * @param <T> Kotlin type of the [Struct]'s value
 */
class Struct<T : Any>(values: Map<String, Expression<BigQueryEngine, *>>?) : Row<BigQueryEngine, T>(values) {
    override fun sql(): String {
        if (values == null) {
            return "NULL"
        }
        return "STRUCT(${values!!.values.joinToString(", ") { value -> value.sql() }})"
    }
}
