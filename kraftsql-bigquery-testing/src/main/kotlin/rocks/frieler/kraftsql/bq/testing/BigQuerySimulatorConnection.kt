package rocks.frieler.kraftsql.bq.testing

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.expressions.Expression
import rocks.frieler.kraftsql.objects.DataRow
import rocks.frieler.kraftsql.testing.engine.SimulatorConnection

class BigQuerySimulatorConnection : SimulatorConnection<BigQueryEngine>() {
    override fun <T> simulateExpression(expression: Expression<BigQueryEngine, T>) : (DataRow) -> T? =
        when (expression) {
            else -> super.simulateExpression(expression)
        }
}
