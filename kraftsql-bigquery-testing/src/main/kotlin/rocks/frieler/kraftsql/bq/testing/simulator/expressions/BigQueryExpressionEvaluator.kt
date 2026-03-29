package rocks.frieler.kraftsql.bq.testing.simulator.expressions

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.expressions.ArrayConcat
import rocks.frieler.kraftsql.testing.simulator.expressions.ArrayConcatenationSimulator
import rocks.frieler.kraftsql.testing.simulator.expressions.GenericExpressionEvaluator
import kotlin.reflect.KClass

object BigQueryExpressionEvaluator : GenericExpressionEvaluator<BigQueryEngine>() {
    init {
        unregisterExpressionSimulator(rocks.frieler.kraftsql.expressions.Constant::class)
        registerExpressionSimulator(ConstantSimulator<Any>())
        unregisterExpressionSimulator(rocks.frieler.kraftsql.expressions.Row::class)
        registerExpressionSimulator(StructSimulator())
        registerExpressionSimulator(NotSimulator())
        registerExpressionSimulator(AndSimulator())
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
