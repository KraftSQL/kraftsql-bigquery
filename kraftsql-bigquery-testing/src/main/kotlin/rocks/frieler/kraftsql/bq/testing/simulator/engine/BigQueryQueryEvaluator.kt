package rocks.frieler.kraftsql.bq.testing.simulator.engine

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.expressions.Constant
import rocks.frieler.kraftsql.bq.testing.simulator.expressions.BigQueryExpressionEvaluator
import rocks.frieler.kraftsql.bq.testing.simulator.expressions.BigQuerySubexpressionCollector
import rocks.frieler.kraftsql.dql.Select
import rocks.frieler.kraftsql.objects.DataRow
import rocks.frieler.kraftsql.testing.simulator.engine.EngineState
import rocks.frieler.kraftsql.testing.simulator.engine.GenericQueryEvaluator
import java.sql.SQLException

object BigQueryQueryEvaluator : GenericQueryEvaluator<BigQueryEngine>(
    orm = BigQuerySimulatorORMapping,
    subexpressionCollector = BigQuerySubexpressionCollector(),
    expressionEvaluator = BigQueryExpressionEvaluator
) {
    init {
        correlatedJoinsEnabled = true
    }

    context(activeState: EngineState<BigQueryEngine>)
    override fun selectRows(select: Select<BigQueryEngine, *>, correlatedData: DataRow?): List<DataRow> {
        if (select.grouping.any { it is Constant }) { throw SQLException("Cannot GROUP BY literal values") }
        return super.selectRows(select, correlatedData)
    }
}
