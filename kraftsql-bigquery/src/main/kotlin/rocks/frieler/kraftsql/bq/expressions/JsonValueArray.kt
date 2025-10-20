package rocks.frieler.kraftsql.bq.expressions

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.expressions.Expression
import java.util.Objects

/**
 * BigQuery's
 * [`JSON_VALUE_ARRAY()`](https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_value_array)
 * function.
 */
class JsonValueArray(
    val jsonString: Expression<BigQueryEngine, String>,
    val jsonPath: Expression<BigQueryEngine, String>? = null,
) : Expression<BigQueryEngine, Array<String>> {
    override fun sql() =
        "JSON_VALUE_ARRAY(${jsonString.sql()}${jsonPath?.let { ", ${it.sql()}"} ?: ""})"

    override fun defaultColumnName() =
        "JSON_VALUE_ARRAY(${jsonString.defaultColumnName()}${jsonPath?.let { ", ${it.defaultColumnName()}"} ?: ""})"

    override fun equals(other: Any?) =
        other is JsonValueArray && other.jsonString == jsonString && other.jsonPath == jsonPath

    override fun hashCode() = Objects.hash(jsonString, jsonPath)
}
