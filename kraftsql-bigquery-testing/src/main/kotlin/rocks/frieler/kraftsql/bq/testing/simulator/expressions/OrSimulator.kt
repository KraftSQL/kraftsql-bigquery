@file:Suppress("IntroduceWhenSubject")

package rocks.frieler.kraftsql.bq.testing.simulator.expressions

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine

class OrSimulator : rocks.frieler.kraftsql.testing.simulator.expressions.OrSimulator<BigQueryEngine>() {
    override fun simulate(left: Boolean?, right: Boolean?) =
        when {
            left == false && right == null -> null
            left == null && right == false -> null
            left == null && right == null -> null
            else -> super.simulate(left, right)
        }
}
