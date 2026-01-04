package rocks.frieler.kraftsql.bq.objects

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.engine.Connection
import rocks.frieler.kraftsql.objects.Data
import rocks.frieler.kraftsql.objects.collect

typealias Data<T> = Data<BigQueryEngine, T>

/**
 * Collects this [Data]'s rows as Kotlin objects using the [BigQueryEngine]'s default connection.
 *
 * @param T the Kotlin type of the rows
 * @return the [Data]'s rows as Kotlin objects
 * @see Connection.collect
 */
inline fun <reified T : Any> Data<BigQueryEngine, T>.collect() = this.collect(BigQueryEngine.DefaultConnection.get())
