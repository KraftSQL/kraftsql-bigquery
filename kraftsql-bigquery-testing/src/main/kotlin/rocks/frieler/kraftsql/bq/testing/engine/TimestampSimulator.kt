package rocks.frieler.kraftsql.bq.testing.engine

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.expressions.Timestamp
import rocks.frieler.kraftsql.expressions.Expression
import rocks.frieler.kraftsql.objects.DataRow
import rocks.frieler.kraftsql.testing.engine.ExpressionSimulator
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneId

/**
 * Simulator for BigQuery's [Timestamp] expression.
 */
class TimestampSimulator : ExpressionSimulator<BigQueryEngine, Instant, Timestamp> {
    override val expression = Timestamp::class

    private val timestampLiteralPattern = "^(?<date>\\d{4}-\\d{1,2}-\\d{1,2})[Tt ](?<time>\\d{1,2}:\\d{1,2}:\\d{1,2}(.\\d{1,6})?)?(?<tz>|[Zz]|[+-]\\d{1,2}(:\\d{2})?| .+/.+)$".toPattern()

    context(subexpressionCallbacks : ExpressionSimulator.SubexpressionCallbacks<BigQueryEngine>)
    override fun simulateExpression(expression: Timestamp): (DataRow) -> Instant? = { row ->
        simulate(subexpressionCallbacks.simulateExpression(expression.stringExpression)(row))
    }

    context(groupExpressions: List<Expression<BigQueryEngine, *>>, subexpressionCallbacks : ExpressionSimulator.SubexpressionCallbacks<BigQueryEngine>)
    override fun simulateAggregation(expression: Timestamp): (List<DataRow>) -> Instant? = { rows ->
        simulate(subexpressionCallbacks.simulateAggregation(expression.stringExpression)(rows))
    }

    private fun simulate(timestamp: String?): Instant? =
        if (timestamp == null) {
            null
        } else {
            val matcher = timestampLiteralPattern.matcher(timestamp).also {
                if (!it.matches()) throw IllegalArgumentException("invalid timestamp format: $timestamp")
            }
            LocalDateTime
                .parse("${matcher.group("date")}T${matcher.group("time") ?: "00:00:00.000000"}")
                .atZone(ZoneId.of(matcher.group("tz").trim().let { if(it == "z") "Z" else it }.ifEmpty { null } ?: "Z"))
                .toInstant()
        }
}
