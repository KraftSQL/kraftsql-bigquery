package rocks.frieler.kraftsql.bq.testing

import com.jayway.jsonpath.JsonPath
import rocks.frieler.kraftsql.bq.engine.BigQueryConnection
import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.expressions.JsonValue
import rocks.frieler.kraftsql.bq.expressions.JsonValueArray
import rocks.frieler.kraftsql.bq.expressions.Replace
import rocks.frieler.kraftsql.bq.expressions.Timestamp
import rocks.frieler.kraftsql.expressions.Expression
import rocks.frieler.kraftsql.objects.DataRow
import rocks.frieler.kraftsql.testing.engine.GenericSimulatorConnection
import java.time.Instant

class BigQuerySimulatorConnection : BigQueryConnection, GenericSimulatorConnection<BigQueryEngine>() {
    private val timestampLiteralPattern = "^(?<date>\\d{4}-\\d{1,2}-\\d{1,2})[Tt ](?<time>\\d{1,2}:\\d{1,2}:\\d{1,2}(.\\d{1,6})?)?(?<tz>|[Zz]|[+-]\\d{1,2}(:\\d{2})?| .+/.+)$".toPattern()

    override fun <T> simulateExpression(expression: Expression<BigQueryEngine, T>) : (DataRow) -> T? =
        when (expression) {
            is Replace -> { row ->
                val originalValue = simulateExpression(expression.originalValue).invoke(row)
                val fromPattern = simulateExpression(expression.fromPattern).invoke(row)!!
                @Suppress("UNCHECKED_CAST")
                if (fromPattern.isEmpty()) {
                    originalValue
                } else {
                    val toPattern = simulateExpression(expression.toPattern).invoke(row)!!
                    originalValue?.replace(fromPattern, toPattern)
                } as T?
            }
            is Timestamp -> { row ->
                val timestamp = simulateExpression(expression.stringExpression).invoke(row)
                @Suppress("UNCHECKED_CAST")
                timestamp?.let {
                    val matcher = timestampLiteralPattern.matcher(it)
                    if (!matcher.matches()) {
                        throw IllegalArgumentException("invalid timestamp format: $it")
                    }
                    val canonicalTimestamp = "${matcher.group("date")}" +
                            "T${matcher.group("time") ?: "00:00:00.000000"}" +
                            (matcher.group("tz").trim().ifEmpty { null } ?: "Z")
                    Instant.parse(canonicalTimestamp)
                } as T?
            }
            is JsonValue -> { row ->
                val jsonString = simulateExpression(expression.jsonString).invoke(row)
                val jsonPath = expression.jsonPath?.let { simulateExpression(it).invoke(row) }
                @Suppress("UNCHECKED_CAST")
                JsonPath.read<String>(jsonString, jsonPath ?: "$") as T?
            }
            is JsonValueArray -> { row ->
                val jsonString = simulateExpression(expression.jsonString).invoke(row).let { if (it.isNullOrBlank()) "[]" else it }
                val jsonPath = expression.jsonPath?.let { simulateExpression(it).invoke(row) }
                @Suppress("UNCHECKED_CAST")
                JsonPath.read<List<Any>>(jsonString, jsonPath ?: "$").map { it.toString() }.toTypedArray() as T?
            }
            else -> super.simulateExpression(expression)
        }
}
