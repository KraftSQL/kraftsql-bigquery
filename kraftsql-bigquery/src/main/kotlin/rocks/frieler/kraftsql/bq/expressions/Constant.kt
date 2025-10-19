package rocks.frieler.kraftsql.bq.expressions

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.engine.Types
import rocks.frieler.kraftsql.expressions.Constant
import java.math.BigDecimal

/**
 * BigQuery-specific replacement of [rocks.frieler.kraftsql.expressions.Constant].
 *
 * @param <T> the Kotlin type of the [Constant] value
 * @param value the constant value
 */open class Constant<T : Any>(
    value: T?,
) : Constant<BigQueryEngine, T>(value) {
    override fun sql(): String {
        return when (value) {
            is BigDecimal -> "${Types.BIGNUMERIC.sql()} '${value}'"
            else -> super.sql()
        }
    }
}
