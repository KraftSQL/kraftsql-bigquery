package rocks.frieler.kraftsql.bq.testing.simulator.expressions

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine

class NotSimulator : rocks.frieler.kraftsql.testing.simulator.expressions.NotSimulator<BigQueryEngine>() {
    override fun simulate(value: Boolean?) = value?.not()
}
