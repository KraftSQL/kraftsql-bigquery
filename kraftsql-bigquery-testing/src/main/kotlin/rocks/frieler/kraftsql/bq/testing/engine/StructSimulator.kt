package rocks.frieler.kraftsql.bq.testing.engine

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.expressions.Struct
import rocks.frieler.kraftsql.objects.DataRow
import rocks.frieler.kraftsql.testing.engine.RowSimulator
import kotlin.reflect.KClass

/**
 * Simulator for BigQuery's [Struct] expression.
 */
class StructSimulator : RowSimulator<BigQueryEngine>() {
    @Suppress("UNCHECKED_CAST")
    override val expression = Struct::class as KClass<out Struct<DataRow?>>
}
