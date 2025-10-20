package rocks.frieler.kraftsql.bq.expressions

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.expressions.Expression
import java.util.Objects

/**
 * BigQuery's [`JSON_VALUE()`](https://cloud.google.com/bigquery/docs/reference/standard-sql/json_functions#json_value)
 * function.
 *
 * @param jsonString the expression to extract the JSON value from
 * @param jsonPath optional JSONPath expression to select a certain node in the JSON document
 */
class JsonValue(
    val jsonString: Expression<BigQueryEngine, String>,
    val jsonPath: Expression<BigQueryEngine, String>? = null,
) : Expression<BigQueryEngine, String> {
    override fun sql() =
        "JSON_VALUE(${jsonString.sql()}${jsonPath?.let { ", ${it.sql()}"} ?: ""})"

    override fun defaultColumnName() =
        "JSON_VALUE(${jsonString.defaultColumnName()}${jsonPath?.let { ", ${it.defaultColumnName()}"} ?: ""})"

    override fun equals(other: Any?) =
        other is JsonValue && other.jsonString == jsonString && other.jsonPath == jsonPath

    override fun hashCode() = Objects.hash(jsonString, jsonPath)
}
