package rocks.frieler.kraftsql.bq.expressions

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.expressions.Expression
import kotlin.Array

/**
 * The `ARRAY_LENGTH()` function, that returns the length of an array [Expression].
 *
 * This replaces [rocks.frieler.kraftsql.expressions.ArrayLength], because BigQuery's `ARRAY_LENGTH()` function returns
 * an Int64.
 *
 * @param array the array [Expression] to get the length of
 */
class ArrayLength(
    val array: Expression<BigQueryEngine, Array<*>?>,
) : Expression<BigQueryEngine, Long?> {
    override fun sql() = "ARRAY_LENGTH(${array.sql()})"

    override fun defaultColumnName() = "ARRAY_LENGTH(${array.defaultColumnName()})"

    override fun equals(other: Any?) = other is ArrayLength
            && array == other.array

    override fun hashCode() = array.hashCode()
}
