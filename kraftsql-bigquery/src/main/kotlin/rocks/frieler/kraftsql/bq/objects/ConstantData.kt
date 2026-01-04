package rocks.frieler.kraftsql.bq.objects

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.engine.BigQueryORMapping
import rocks.frieler.kraftsql.engine.Engine
import rocks.frieler.kraftsql.engine.ORMapping
import rocks.frieler.kraftsql.objects.ConstantData

/**
 * Constant [Data] in BigQuery.
 *
 * @param <T> the Kotlin type of the [ConstantData]'s items, either a data class or generically
 * [rocks.frieler.kraftsql.objects.DataRow]
 */
class ConstantData<T : Any> : ConstantData<BigQueryEngine, T> {
    /**
     * Creates a new [ConstantData] with the given items.
     *
     * The items must not be empty and all be of the same class, either a data-class or
     * [rocks.frieler.kraftsql.objects.DataRow].
     *
     * @param orm the [ORMapping] of the [Engine]
     * @param items the rows
     */
    constructor(items: Iterable<T>) : super(BigQueryORMapping, items)

    /**
     * Creates a new [ConstantData] with the given items.
     *
     * The items must not be empty and all be of the same class, either a data-class or
     * [rocks.frieler.kraftsql.objects.DataRow].
     *
     * @param orm the [ORMapping] of the [Engine]
     * @param items the rows
     */
    constructor(vararg items: T) : super(BigQueryORMapping, *items)

    private constructor(columnNames: List<String>) : super(BigQueryORMapping, emptyList(), columnNames)

    companion object {
        /**
         * Creates a new empty [ConstantData] with the given column names (although there are no rows).
         *
         * Note: This function is mainly intended for use with (no) [rocks.frieler.kraftsql.objects.DataRow]s. In case
         * of (no) instances of a data-class, you should prefer the overloaded version that derives the column names
         * from that class.
         *
         * @param <T> the Kotlin type of the [ConstantData]'s items, either a data class or generically
         * [rocks.frieler.kraftsql.objects.DataRow]
         */
        fun <T : Any> empty(columnNames: List<String>) = ConstantData<T>(columnNames)

        /**
         * Creates a new empty [ConstantData] deriving the column names from the row type (although there are no rows)
         * using [ORMapping.getSchemaFor].
         *
         * @param <T> the Kotlin type of the [ConstantData]'s items; must be a data class
         */
        inline fun <reified T : Any> empty() = empty<T>(BigQueryORMapping.getSchemaFor(T::class).map { it.name })
    }
}
