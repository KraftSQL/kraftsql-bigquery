package rocks.frieler.kraftsql.bq.testing.engine

import com.jayway.jsonpath.JsonPath
import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.expressions.JsonValueArray
import rocks.frieler.kraftsql.expressions.Expression
import rocks.frieler.kraftsql.objects.DataRow
import rocks.frieler.kraftsql.testing.engine.ExpressionSimulator
import kotlin.text.isNullOrBlank

/**
 * Simulator for BigQuery's [JsonValueArray] function.
 */
class JsonValueArraySimulator : ExpressionSimulator<BigQueryEngine, Array<String>?, JsonValueArray> {
    override val expression = JsonValueArray::class

    context(subexpressionCallbacks: ExpressionSimulator.SubexpressionCallbacks<BigQueryEngine>)
    override fun simulateExpression(expression: JsonValueArray): (DataRow) -> Array<String>? = { row ->
        val jsonString = subexpressionCallbacks.simulateExpression(expression.jsonString)(row)
        val jsonPath = expression.jsonPath?.let { subexpressionCallbacks.simulateExpression(it)(row) }
        simulate(jsonString, jsonPath)
    }

    context(groupExpressions: List<Expression<BigQueryEngine, *>>, subexpressionCallbacks: ExpressionSimulator.SubexpressionCallbacks<BigQueryEngine>)
    override fun simulateAggregation(expression: JsonValueArray): (List<DataRow>) -> Array<String>? = { rows ->
        val jsonString = subexpressionCallbacks.simulateAggregation(expression.jsonString)(rows)
        val jsonPath = expression.jsonPath?.let { subexpressionCallbacks.simulateAggregation(it)(rows) }
        simulate(jsonString, jsonPath)
    }

    private fun simulate(jsonString: String?, jsonPath: String?): Array<String>? =
        jsonString
            .let { if (it.isNullOrBlank()) "[]" else it }
            .let { JsonPath.read<List<Any>>(it, jsonPath ?: "$") as? List }
            ?.takeIf { it.all { element -> element::class.javaPrimitiveType != null || element is String } }
            ?.map { it.toString() }
            ?.toTypedArray()
}
