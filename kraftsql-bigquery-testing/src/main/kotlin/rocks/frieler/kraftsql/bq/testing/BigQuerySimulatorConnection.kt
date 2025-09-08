package rocks.frieler.kraftsql.bq.testing

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.expressions.Replace
import rocks.frieler.kraftsql.expressions.Expression
import rocks.frieler.kraftsql.objects.DataRow
import rocks.frieler.kraftsql.testing.engine.SimulatorConnection

class BigQuerySimulatorConnection : SimulatorConnection<BigQueryEngine>() {
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
            else -> super.simulateExpression(expression)
        }
}
