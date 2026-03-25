package rocks.frieler.kraftsql.bq.testing.simulator.engine

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.testing.simulator.expressions.BigQueryExpressionEvaluator
import rocks.frieler.kraftsql.bq.testing.simulator.expressions.BigQuerySubexpressionCollector
import rocks.frieler.kraftsql.testing.simulator.engine.GenericQueryEvaluator

object BigQueryQueryEvaluator : GenericQueryEvaluator<BigQueryEngine>(
    orm = BigQuerySimulatorORMapping,
    subexpressionCollector = BigQuerySubexpressionCollector(),
    expressionEvaluator = BigQueryExpressionEvaluator
) {
    init {
        correlatedJoinsEnabled = true
    }
}
