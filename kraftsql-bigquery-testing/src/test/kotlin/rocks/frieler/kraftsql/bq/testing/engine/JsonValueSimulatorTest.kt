package rocks.frieler.kraftsql.bq.testing.engine

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.expressions.JsonValue
import rocks.frieler.kraftsql.expressions.Expression
import rocks.frieler.kraftsql.testing.engine.ExpressionSimulator

class JsonValueSimulatorTest {
    private val subexpressionCallbacks = mock<ExpressionSimulator.SubexpressionCallbacks<BigQueryEngine>>()

    @Test
    fun `JsonValueSimulator parses NULL string to NULL`() {
        val simulation = context(subexpressionCallbacks) {
            JsonValueSimulator().simulateExpression(JsonValue(
                mock { whenever(subexpressionCallbacks.simulateExpression(it)).thenReturn { _ -> null } },
            ))
        }
        val result = simulation(mock())

        result shouldBe null
    }

    @Test
    fun `JsonValueSimulator parses scalar string JSON value as string`() {
        val simulation = context(subexpressionCallbacks) {
            JsonValueSimulator().simulateExpression(JsonValue(
                mock { whenever(subexpressionCallbacks.simulateExpression(it)).thenReturn { _ -> "foo" } },
            ))
        }
        val result = simulation(mock())

        result shouldBe "foo"
    }

    @Test
    fun `JsonValueSimulator parses scalar non-string JSON value as string`() {
        val simulation = context(subexpressionCallbacks) {
            JsonValueSimulator().simulateExpression(JsonValue(
                mock { whenever(subexpressionCallbacks.simulateExpression(it)).thenReturn { _ -> "true" } },
            ))
        }
        val result = simulation(mock())

        result shouldBe "true"
    }

    @Test
    fun `JsonValueSimulator returns NULL for non scalar JSON node`() {
        val simulation = context(subexpressionCallbacks) {
            JsonValueSimulator().simulateExpression(JsonValue(
                mock { whenever(subexpressionCallbacks.simulateExpression(it)).thenReturn { _ -> "{ \"foo\": \"bar\" }" } },
            ))
        }
        val result = simulation(mock())

        result shouldBe null
    }

    @Test
    fun `JsonValueSimulator parses scalar JSON node selected by JSONPath as string`() {
        val simulation = context(subexpressionCallbacks) {
            JsonValueSimulator().simulateExpression(JsonValue(
                mock { whenever(subexpressionCallbacks.simulateExpression(it)).thenReturn { _ -> "{ \"foo\": \"bar\" }" } },
                mock { whenever(subexpressionCallbacks.simulateExpression(it)).thenReturn { _ -> "$.foo" } },
            ))
        }
        val result = simulation(mock())

        result shouldBe "bar"
    }

    @Test
    fun `JsonValueSimulator can simulate JsonValue wrapping aggregations`() {
        val simulation = context(emptyList<Expression<BigQueryEngine, *>>(), subexpressionCallbacks) {
            JsonValueSimulator().simulateAggregation(JsonValue(
                mock { whenever(subexpressionCallbacks.simulateAggregation(it)).thenReturn { _ -> "{ \"foo\": \"bar\" }" } },
                mock { whenever(subexpressionCallbacks.simulateAggregation(it)).thenReturn { _ -> "$.foo" } },
            ))
        }
        val result = simulation(mock())

        result shouldBe "bar"
    }
}
