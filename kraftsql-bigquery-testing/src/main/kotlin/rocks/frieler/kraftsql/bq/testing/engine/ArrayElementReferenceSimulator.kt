package rocks.frieler.kraftsql.bq.testing.engine

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.testing.engine.ArrayElementReferenceSimulator

/**
 * [ArrayElementReferenceSimulator] for BigQuery, which implements 0-based arrays.
 *
 * @param <T> the Kotlin type of the array's elements
 */
class ArrayElementReferenceSimulator<T> : ArrayElementReferenceSimulator<BigQueryEngine, T>() {
    override fun simulate(array: Array<T>?, index: Int) = super.simulate(array, index + 1)
}
