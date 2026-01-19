package rocks.frieler.kraftsql.bq.testing.engine

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.expressions.Unnest
import rocks.frieler.kraftsql.bq.objects.ConstantData
import rocks.frieler.kraftsql.bq.objects.Data
import rocks.frieler.kraftsql.expressions.Expression
import rocks.frieler.kraftsql.objects.DataRow
import rocks.frieler.kraftsql.testing.engine.ExpressionSimulator
import kotlin.reflect.KClass

class UnnestSimulator<T : Any> : ExpressionSimulator<BigQueryEngine, Data<T>, Unnest<T>> {
    @Suppress("UNCHECKED_CAST")
    override val expression = Unnest::class as KClass<out Unnest<T>>

    context(subexpressionCallbacks: ExpressionSimulator.SubexpressionCallbacks<BigQueryEngine>)
    override fun simulateExpression(expression: Unnest<T>): (DataRow) -> Data<T> {
        val expressionToUnnest = subexpressionCallbacks.simulateExpression(expression.arrayExpression)
        return { row -> simulate(expressionToUnnest(row)) }
    }

    context(groupExpressions: List<Expression<BigQueryEngine, *>>, subexpressionCallbacks: ExpressionSimulator.SubexpressionCallbacks<BigQueryEngine>)
    override fun simulateAggregation(expression: Unnest<T>): (List<DataRow>) -> Data<T> {
        val expressionToUnnest = subexpressionCallbacks.simulateAggregation(expression.arrayExpression)
        return { rows -> simulate(expressionToUnnest(rows)) }
    }

    private fun simulate(array: Array<out T>?): Data<T> =
        if (array.isNullOrEmpty()) ConstantData.empty(emptyList()) else ConstantData(array.toList())
}
