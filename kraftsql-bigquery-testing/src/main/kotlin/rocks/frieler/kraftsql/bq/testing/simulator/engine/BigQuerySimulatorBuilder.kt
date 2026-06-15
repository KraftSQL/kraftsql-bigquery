package rocks.frieler.kraftsql.bq.testing.simulator.engine

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.expressions.ArrayConcat
import rocks.frieler.kraftsql.bq.testing.simulator.expressions.AndSimulator
import rocks.frieler.kraftsql.bq.testing.simulator.expressions.ArrayElementReferenceSimulator
import rocks.frieler.kraftsql.bq.testing.simulator.expressions.ArrayLengthSimulator
import rocks.frieler.kraftsql.bq.testing.simulator.expressions.BigQuerySubexpressionCollector
import rocks.frieler.kraftsql.bq.testing.simulator.expressions.ConstantSimulator
import rocks.frieler.kraftsql.bq.testing.simulator.expressions.JsonValueArraySimulator
import rocks.frieler.kraftsql.bq.testing.simulator.expressions.JsonValueSimulator
import rocks.frieler.kraftsql.bq.testing.simulator.expressions.NotSimulator
import rocks.frieler.kraftsql.bq.testing.simulator.expressions.OrSimulator
import rocks.frieler.kraftsql.bq.testing.simulator.expressions.ReplaceSimulator
import rocks.frieler.kraftsql.bq.testing.simulator.expressions.StructSimulator
import rocks.frieler.kraftsql.bq.testing.simulator.expressions.TimestampSimulator
import rocks.frieler.kraftsql.bq.testing.simulator.expressions.UnnestSimulator
import rocks.frieler.kraftsql.testing.simulator.engine.GenericEngineSimulatorBuilderTemplate
import rocks.frieler.kraftsql.testing.simulator.engine.GenericQueryEvaluator
import rocks.frieler.kraftsql.testing.simulator.expressions.ArrayConcatenationSimulator
import kotlin.reflect.KClass

/**
 * Builder that assembles a [BigQuerySimulator] for the [BigQueryEngine].
 *
 * It wires up the BigQuery-specific [orm][BigQuerySimulatorORMapping] and [queryEvaluator][BigQueryQueryEvaluator] and
 * adapts the registered [rocks.frieler.kraftsql.testing.simulator.expressions.ExpressionSimulator]s to BigQuery's
 * behavior.
 */
class BigQuerySimulatorBuilder : GenericEngineSimulatorBuilderTemplate<BigQueryEngine, BigQuerySimulator>() {
    override val orm = BigQuerySimulatorORMapping
    override val queryEvaluator: GenericQueryEvaluator<BigQueryEngine> =
        BigQueryQueryEvaluator(orm, BigQuerySubexpressionCollector(), expressionEvaluator)

    override fun registerExpressionSimulators() {
        super.registerExpressionSimulators()

        expressionEvaluator.apply {
            unregisterExpressionSimulator(rocks.frieler.kraftsql.expressions.Constant::class)
            registerExpressionSimulator(ConstantSimulator<Any>())
            unregisterExpressionSimulator(rocks.frieler.kraftsql.expressions.Row::class)
            registerExpressionSimulator(StructSimulator())
            registerExpressionSimulator(NotSimulator())
            registerExpressionSimulator(AndSimulator())
            registerExpressionSimulator(OrSimulator())
            registerExpressionSimulator(ReplaceSimulator())
            registerExpressionSimulator(TimestampSimulator())
            registerExpressionSimulator(ArrayElementReferenceSimulator<Any?>())
            unregisterExpressionSimulator(rocks.frieler.kraftsql.expressions.ArrayLength::class)
            registerExpressionSimulator(ArrayLengthSimulator())
            @Suppress("UNCHECKED_CAST")
            registerExpressionSimulator(ArrayConcatenationSimulator(ArrayConcat::class as KClass<out ArrayConcat<Any?>>))
            registerExpressionSimulator(JsonValueSimulator())
            registerExpressionSimulator(JsonValueArraySimulator())
            registerExpressionSimulator(UnnestSimulator<Any>())
        }
    }

    override fun makeSimulator() = BigQuerySimulator(orm, persistentState, expressionEvaluator, queryEvaluator)
}
