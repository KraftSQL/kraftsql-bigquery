package rocks.frieler.kraftsql.bq.testing.simulator.expressions

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine

/**
 * [ArrayElementReferenceSimulator] for BigQuery, which implements 0-based arrays.
 *
 * @param <T> the Kotlin type of the array's elements
 */
class ArrayElementReferenceSimulator<T> : rocks.frieler.kraftsql.testing.simulator.expressions.ArrayElementReferenceSimulator<BigQueryEngine, T>() {
    override fun simulate(array: Array<T>?, index: Int) = super.simulate(array, index + 1)
}
