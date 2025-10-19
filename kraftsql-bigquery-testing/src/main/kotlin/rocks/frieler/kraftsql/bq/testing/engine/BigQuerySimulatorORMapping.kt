package rocks.frieler.kraftsql.bq.testing.engine

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.engine.BigQueryORMapping
import rocks.frieler.kraftsql.testing.engine.SimulatorORMapping

object BigQuerySimulatorORMapping : SimulatorORMapping<BigQueryEngine>() {
    override fun <T : Any> serialize(value: T?) = BigQueryORMapping.serialize(value)
}
