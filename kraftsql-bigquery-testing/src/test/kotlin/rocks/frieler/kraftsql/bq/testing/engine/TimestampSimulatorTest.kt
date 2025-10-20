package rocks.frieler.kraftsql.bq.testing.engine

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.expressions.Timestamp
import rocks.frieler.kraftsql.expressions.Expression
import rocks.frieler.kraftsql.testing.engine.ExpressionSimulator
import java.time.Instant

class TimestampSimulatorTest {
    private val subexpressionCallbacks = mock<ExpressionSimulator.SubexpressionCallbacks<BigQueryEngine>>()

    @Test
    fun `TimestampSimulator parses NULL string to NULL`() {
        val simulation = context(subexpressionCallbacks) {
            TimestampSimulator().simulateExpression(Timestamp(
                mock { whenever(subexpressionCallbacks.simulateExpression(it)).thenReturn { _ -> null } },
            ))
        }
        val result = simulation(mock())

        result shouldBe null
    }

    @Test
    fun `TimestampSimulator parses canonical timestamp`() {
        val simulation = context(subexpressionCallbacks) {
            TimestampSimulator().simulateExpression(Timestamp(
                mock { whenever(subexpressionCallbacks.simulateExpression(it)).thenReturn { _ -> "2025-10-20T20:01:29.123456Z" } },
            ))
        }
        val result = simulation(mock())

        result shouldBe Instant.parse("2025-10-20T20:01:29.123456Z")
    }

    @Test
    fun `TimestampSimulator parses timestamp String with lower 't'`() {
        val simulation = context(subexpressionCallbacks) {
            TimestampSimulator().simulateExpression(Timestamp(
                mock { whenever(subexpressionCallbacks.simulateExpression(it)).thenReturn { _ -> "2025-10-20t20:01:29.123456Z" } },
            ))
        }
        val result = simulation(mock())

        result shouldBe Instant.parse("2025-10-20T20:01:29.123456Z")
    }

    @Test
    fun `TimestampSimulator parses timestamp String with space between date and time`() {
        val simulation = context(subexpressionCallbacks) {
            TimestampSimulator().simulateExpression(Timestamp(
                mock { whenever(subexpressionCallbacks.simulateExpression(it)).thenReturn { _ -> "2025-10-20 20:01:29.123456Z" } },
            ))
        }
        val result = simulation(mock())

        result shouldBe Instant.parse("2025-10-20T20:01:29.123456Z")
    }

    @Test
    fun `TimestampSimulator parses timestamp String without fractional seconds`() {
        val simulation = context(subexpressionCallbacks) {
            TimestampSimulator().simulateExpression(Timestamp(
                mock { whenever(subexpressionCallbacks.simulateExpression(it)).thenReturn { _ -> "2025-10-20T20:01:29Z" } },
            ))
        }
        val result = simulation(mock())

        result shouldBe Instant.parse("2025-10-20T20:01:29.000000Z")
    }

    @Test
    fun `TimestampSimulator parses timestamp String with lower 'z' as timezone`() {
        val simulation = context(subexpressionCallbacks) {
            TimestampSimulator().simulateExpression(Timestamp(
                mock { whenever(subexpressionCallbacks.simulateExpression(it)).thenReturn { _ -> "2025-10-20T20:01:29z" } },
            ))
        }
        val result = simulation(mock())

        result shouldBe Instant.parse("2025-10-20T20:01:29.000000Z")
    }

    @Test
    fun `TimestampSimulator parses timestamp String without timezone`() {
        val simulation = context(subexpressionCallbacks) {
            TimestampSimulator().simulateExpression(Timestamp(
                mock { whenever(subexpressionCallbacks.simulateExpression(it)).thenReturn { _ -> "2025-10-20T20:01:29" } },
            ))
        }
        val result = simulation(mock())

        result shouldBe Instant.parse("2025-10-20T20:01:29.000000Z")
    }

    @Test
    fun `TimestampSimulator parses timestamp String with offset timezone`() {
        val simulation = context(subexpressionCallbacks) {
            TimestampSimulator().simulateExpression(Timestamp(
                mock { whenever(subexpressionCallbacks.simulateExpression(it)).thenReturn { _ -> "2025-10-20T20:01:29+01:30" } },
            ))
        }
        val result = simulation(mock())

        result shouldBe Instant.parse("2025-10-20T18:31:29.000000Z")
    }

    @Test
    fun `TimestampSimulator parses timestamp String with named timezone`() {
        val simulation = context(subexpressionCallbacks) {
            TimestampSimulator().simulateExpression(Timestamp(
                mock { whenever(subexpressionCallbacks.simulateExpression(it)).thenReturn { _ -> "2025-10-20T20:01:29 Europe/Berlin" } },
            ))
        }
        val result = simulation(mock())

        result shouldBe Instant.parse("2025-10-20T18:01:29.000000Z")
    }

    @Test
    fun `TimestampSimulator rejects invalid timestamp String`() {
        shouldThrow<IllegalArgumentException> {
            context(subexpressionCallbacks) {
                TimestampSimulator().simulateExpression(Timestamp(
                    mock { whenever(subexpressionCallbacks.simulateExpression(it)).thenReturn { _ -> "right now" } },
                ))
            }(mock())
        }
    }

    @Test
    fun `TimestampSimulator can simulate Timestamp wrapping an aggregation`() {
        val simulation = context(emptyList<Expression<BigQueryEngine, *>>(), subexpressionCallbacks) {
            TimestampSimulator().simulateAggregation(Timestamp(
                mock { whenever(subexpressionCallbacks.simulateAggregation(it)).thenReturn { _ -> "2025-10-20T20:01:29.123456Z" } },
            ))
        }
        val result = simulation(listOf(mock()))

        result shouldBe Instant.parse("2025-10-20T20:01:29.123456Z")
    }
}
