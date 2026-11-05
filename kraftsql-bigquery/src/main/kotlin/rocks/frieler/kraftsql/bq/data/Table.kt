package rocks.frieler.kraftsql.bq.data

import rocks.frieler.kraftsql.data.Column
import rocks.frieler.kraftsql.engine.DataClassSerDe

/**
 * A table in BigQuery, identified by its [project], [dataset] and [name].
 *
 * Behaves like a [core Table][rocks.frieler.kraftsql.data.Table], with BigQuery's [project] as [database] and its
 * [dataset] as [namespace], only offering BigQuery's names for these coordinates.
 *
 * Usually, a [Table] of data-class instances is created using the [Table] function, which derives the [schema] from the
 * data class.
 *
 * @param T the Kotlin type of the [Table]'s rows, either a data class or generically
 * [DataRow][rocks.frieler.kraftsql.data.DataRow]
 * @param project the ID of the Google Cloud project containing the [dataset], or `null` for the default project of the
 * [BigQueryEngine][rocks.frieler.kraftsql.bq.engine.BigQueryEngine]
 * @param dataset the name of the dataset containing the table
 * @param name the name of the table
 * @param schema the (possibly incomplete) [Column] definitions of the table
 */
open class Table<T : Any>(
    project: String? = null,
    dataset: String,
    name: String,
    schema: List<Column>,
) : rocks.frieler.kraftsql.data.Table<T>(project, dataset, name, schema) {
    constructor(dataset: String, name: String, schema: List<Column>) : this(null, dataset, name, schema)

    /**
     * The ID of the Google Cloud project containing this [Table], i.e. the [database].
     */
    val project: String? get() = database

    /**
     * The name of the dataset containing the table, i.e. the [namespace].
     */
    val dataset: String get() = namespace
}

/**
 * Creates a BigQuery [Table] of data-class instances.
 *
 * The [schema][Table.schema] of the [Table] is derived from [T] by [DataClassSerDe.getSchemaFor], so the same
 * requirements apply.
 *
 * @param T the data class of the [Table]'s rows
 * @param project the ID of the Google Cloud project containing the [dataset], or `null` for the default project of the
 * [BigQueryEngine][rocks.frieler.kraftsql.bq.engine.BigQueryEngine]
 * @param dataset the name of the dataset containing the table
 * @param name the name of the table
 * @return a [Table] with the [schema][Table.schema] of the data class [T]
 */
inline fun <reified T : Any> Table(project: String? = null, dataset: String, name: String): Table<T> =
    Table(project, dataset, name, DataClassSerDe.getSchemaFor(T::class))

/**
 * Creates a BigQuery [Table] of data-class instances in the default project of the
 * [Engine][rocks.frieler.kraftsql.engine.Engine].
 *
 * The [schema][Table.schema] of the [Table] is derived from [T] by [DataClassSerDe.getSchemaFor], so the same
 * requirements apply.
 *
 * @param T the data class of the [Table]'s rows
 * @param dataset the name of the namespace containing the table, i.e. what SQL calls a schema
 * @param name the name of the table
 * @return a [Table] with the [schema][Table.schema] of the data class [T]
 */
inline fun <reified T : Any> Table(dataset: String, name: String): Table<T> =
    Table(null, dataset, name, DataClassSerDe.getSchemaFor(T::class))
