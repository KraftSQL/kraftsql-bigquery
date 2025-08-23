package rocks.frieler.kraftsql.bq.expressions

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.engine.Types
import rocks.frieler.kraftsql.engine.Engine
import rocks.frieler.kraftsql.expressions.Constant
import java.math.BigDecimal

open class Constant<E : Engine<E>, T : Any>(
    value: T?,
) : Constant<BigQueryEngine, T>(value) {
    override fun sql(): String {
        return when (value) {
            is BigDecimal -> "${Types.BIGNUMERIC.sql()} '${value}'"
            else -> super.sql()
        }
    }
}
