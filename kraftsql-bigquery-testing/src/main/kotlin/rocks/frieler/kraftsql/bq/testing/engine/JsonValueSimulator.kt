package rocks.frieler.kraftsql.bq.testing.engine

import com.jayway.jsonpath.JsonPath
import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.expressions.JsonValue
import rocks.frieler.kraftsql.expressions.Expression
import rocks.frieler.kraftsql.objects.DataRow
import rocks.frieler.kraftsql.testing.engine.ExpressionSimulator

/**
 * Simulator for BigQuery's [JsonValue] function.
 */
class JsonValueSimulator : ExpressionSimulator<BigQueryEngine, String?, JsonValue> {
    override val expression = JsonValue::class

    context(subexpressionCallbacks: ExpressionSimulator.SubexpressionCallbacks<BigQueryEngine>)
    override fun simulateExpression(expression: JsonValue): (DataRow) -> String? = { row ->
        simulate(
            subexpressionCallbacks.simulateExpression(expression.jsonString)(row),
            expression.jsonPath?.let { subexpressionCallbacks.simulateExpression(it)(row) })
    }

    context(groupExpressions: List<Expression<BigQueryEngine, *>>, subexpressionCallbacks: ExpressionSimulator.SubexpressionCallbacks<BigQueryEngine>)
    override fun simulateAggregation(expression: JsonValue): (List<DataRow>) -> String? = { rows ->
        simulate(
            subexpressionCallbacks.simulateAggregation(expression.jsonString)(rows),
            expression.jsonPath?.let { subexpressionCallbacks.simulateAggregation(it)(rows) })
    }

    private fun simulate(jsonString: String?, jsonPath: String?): String? =
        jsonString?.let { JsonPath.read<Any>(it, jsonPath ?: "$") as? String }
}
