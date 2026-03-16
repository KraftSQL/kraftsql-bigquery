package rocks.frieler.kraftsql.bq.testing.simulator.expressions

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.expressions.Constant
import kotlin.reflect.KClass

/**
 * [ConstantSimulator] for the BigQuery specific [Constant].
 *
 * @param <T> the Kotlin type of the [Constant]
 */
class ConstantSimulator<T : Any> : rocks.frieler.kraftsql.testing.simulator.expressions.ConstantSimulator<BigQueryEngine, T>() {
    @Suppress("UNCHECKED_CAST")
    override val expression = Constant::class as KClass<out Constant<T>>
}
