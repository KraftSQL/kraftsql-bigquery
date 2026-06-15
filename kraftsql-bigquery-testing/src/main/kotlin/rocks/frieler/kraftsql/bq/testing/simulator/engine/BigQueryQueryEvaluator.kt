package rocks.frieler.kraftsql.bq.testing.simulator.engine

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.expressions.Constant
import rocks.frieler.kraftsql.bq.testing.simulator.expressions.BigQuerySubexpressionCollector
import rocks.frieler.kraftsql.dql.Select
import rocks.frieler.kraftsql.expressions.Column
import rocks.frieler.kraftsql.expressions.Expression
import rocks.frieler.kraftsql.objects.DataRow
import rocks.frieler.kraftsql.testing.simulator.engine.EngineState
import rocks.frieler.kraftsql.testing.simulator.engine.GenericQueryEvaluator
import rocks.frieler.kraftsql.testing.simulator.expressions.GenericExpressionEvaluator
import java.sql.SQLException

class BigQueryQueryEvaluator(
    orm: BigQuerySimulatorORMapping,
    subexpressionCollector: BigQuerySubexpressionCollector,
    expressionEvaluator: GenericExpressionEvaluator<BigQueryEngine>,
) : GenericQueryEvaluator<BigQueryEngine>(orm, subexpressionCollector, expressionEvaluator) {
    init {
        correlatedJoinsEnabled = true
    }

    private val internalGeneratedNamePattern = Regex($$"^\\$col\\d+$")

    context(activeState: EngineState<BigQueryEngine>)
    override fun selectRows(select: Select<BigQueryEngine, *>): List<DataRow> =
        fillPublicColumnNames(super.selectRows(select))

    private fun fillPublicColumnNames(rows: List<DataRow>): List<DataRow> {
        val givenColumnNames = rows.firstOrNull()?.columnNames ?: emptyList()
        val takenColumnNames = givenColumnNames.filterNot { it.matches(internalGeneratedNamePattern) }.toMutableList()
        var currentNumberForGeneratedNames = 0
        val generatedNames = mutableMapOf<String, String>()
        givenColumnNames.filter { name -> name.matches(internalGeneratedNamePattern) }.forEach { name ->
            while (takenColumnNames.contains("f${currentNumberForGeneratedNames}_")) currentNumberForGeneratedNames++
            generatedNames[name] = "f${currentNumberForGeneratedNames}_".also { takenColumnNames.add(it) }
        }

        return rows.map { row ->
            DataRow(row.entries.map { (name, value) -> (generatedNames.getOrDefault(name, name)) to value })
        }
    }

    context(activeState: EngineState<BigQueryEngine>)
    override fun evaluateSelectInternal(select: Select<BigQueryEngine, *>, correlatedData: DataRow?): List<DataRow> {
        if (select.grouping.any { it is Constant }) { throw SQLException("Cannot GROUP BY literal values") }
        return super.evaluateSelectInternal(select, correlatedData)
    }

    override fun fillColumnNames(columns: List<Pair<String?, Expression<BigQueryEngine, *>>>): List<Pair<String, Expression<BigQueryEngine, *>>> {
        var currentNumberForGeneratedNames = 1
        return columns.map { (name, expression) ->
            val effectiveName = name
                ?: (expression as? Column)?.name?.substringAfterLast(".")
                ?: $$"$col$${currentNumberForGeneratedNames++}"
            effectiveName to expression
        }
    }
}
