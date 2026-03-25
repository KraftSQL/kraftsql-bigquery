@file:Suppress("IntroduceWhenSubject")

package rocks.frieler.kraftsql.bq.testing.simulator.expressions

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine

class AndSimulator : rocks.frieler.kraftsql.testing.simulator.expressions.AndSimulator<BigQueryEngine>() {
    override fun simulate(left: Boolean?, right: Boolean?) =
        when {
            left == true && right == null -> null
            left == null && right == true -> null
            left == null && right == null -> null
            else -> super.simulate(left, right)
        }
}
