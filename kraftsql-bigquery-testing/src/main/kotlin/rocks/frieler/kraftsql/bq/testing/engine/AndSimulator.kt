@file:Suppress("IntroduceWhenSubject")

package rocks.frieler.kraftsql.bq.testing.engine

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.testing.engine.AndSimulator

class AndSimulator : AndSimulator<BigQueryEngine>() {
    override fun simulate(left: Boolean?, right: Boolean?) =
        when {
            left == true && right == null -> null
            left == null && right == true -> null
            left == null && right == null -> null
            else -> super.simulate(left, right)
        }
}
