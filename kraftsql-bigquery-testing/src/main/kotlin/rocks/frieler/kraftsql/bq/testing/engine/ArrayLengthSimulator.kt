package rocks.frieler.kraftsql.bq.testing.engine

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.engine.Engine
import rocks.frieler.kraftsql.bq.expressions.ArrayLength
import rocks.frieler.kraftsql.expressions.Expression
import rocks.frieler.kraftsql.objects.DataRow
import rocks.frieler.kraftsql.testing.engine.ExpressionSimulator
import kotlin.reflect.KClass

/**
 * [ExpressionSimulator] for [ArrayLength] expressions.
 */
class ArrayLengthSimulator : ExpressionSimulator<BigQueryEngine, Long?, ArrayLength> {
    @Suppress("UNCHECKED_CAST")
    override val expression = ArrayLength::class as KClass<out ArrayLength>

    context(subexpressionCallbacks: ExpressionSimulator.SubexpressionCallbacks<BigQueryEngine>)
    override fun simulateExpression(expression: ArrayLength): (DataRow) -> Long? {
        val arrayExpression = subexpressionCallbacks.simulateExpression(expression.array)
        return { row -> simulate(arrayExpression(row)) }
    }

    context(groupExpressions: List<Expression<BigQueryEngine, *>>, subexpressionCallbacks: ExpressionSimulator.SubexpressionCallbacks<BigQueryEngine>)
    override fun simulateAggregation(expression: ArrayLength): (List<DataRow>) -> Long? {
        val arrayExpression = subexpressionCallbacks.simulateAggregation(expression.array)
        return { rows -> simulate(arrayExpression(rows)) }
    }

    private fun simulate(array : Array<*>?) : Long? = array?.size?.toLong()
}
