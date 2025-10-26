package rocks.frieler.kraftsql.bq.testing.engine

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.expressions.Replace
import rocks.frieler.kraftsql.expressions.Expression
import rocks.frieler.kraftsql.objects.DataRow
import rocks.frieler.kraftsql.testing.engine.ExpressionSimulator

/**
 * Simulator for BigQuery's [Replace] function.
 */
class ReplaceSimulator : ExpressionSimulator<BigQueryEngine, String?, Replace> {
    override val expression = Replace::class

    context(subexpressionCallbacks: ExpressionSimulator.SubexpressionCallbacks<BigQueryEngine>)
    override fun simulateExpression(expression: Replace): (DataRow) -> String? = { row ->
        val originalValue = subexpressionCallbacks.simulateExpression(expression.originalValue)(row)
        val fromPattern = subexpressionCallbacks.simulateExpression(expression.fromPattern)(row)!!
        val toPattern = subexpressionCallbacks.simulateExpression(expression.toPattern)(row)!!
        simulate(originalValue, fromPattern, toPattern)
    }

    context(groupExpressions: List<Expression<BigQueryEngine, *>>, subexpressionCallbacks: ExpressionSimulator.SubexpressionCallbacks<BigQueryEngine>)
    override fun simulateAggregation(expression: Replace): (List<DataRow>) -> String? = { rows ->
        val originalValue = subexpressionCallbacks.simulateAggregation(expression.originalValue)(rows)
        val fromPattern = subexpressionCallbacks.simulateAggregation(expression.fromPattern)(rows)!!
        val toPattern = subexpressionCallbacks.simulateAggregation(expression.toPattern)(rows)!!
        simulate(originalValue, fromPattern, toPattern)
    }

    private fun simulate(originalValue: String?, fromPattern: String, toPattern: String): String? =
        if (fromPattern.isEmpty()) {
            originalValue
        } else {
            originalValue?.replace(fromPattern, toPattern)
        }
}
