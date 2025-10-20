package rocks.frieler.kraftsql.bq.testing.engine

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.expressions.JsonValueArray
import rocks.frieler.kraftsql.expressions.Expression
import rocks.frieler.kraftsql.testing.engine.ExpressionSimulator

class JsonValueArraySimulatorTest {
    private val subexpressionCallbacks = mock<ExpressionSimulator.SubexpressionCallbacks<BigQueryEngine>>()

    @Test
    fun `JsonValueArraySimulator parses NULL string to empty Array`() {
        val simulation = context(subexpressionCallbacks) {
            JsonValueArraySimulator().simulateExpression(JsonValueArray(
                mock { whenever(subexpressionCallbacks.simulateExpression(it)).thenReturn { _ -> null } },
            ))
        }
        val result = simulation(mock())

        result shouldBe emptyArray()
    }


    @Test
    fun `JsonValueArraySimulator parses arrays scalar values as string array`() {
        val simulation = context(subexpressionCallbacks) {
            JsonValueArraySimulator().simulateExpression(JsonValueArray(
                mock { whenever(subexpressionCallbacks.simulateExpression(it)).thenReturn { _ -> "[\"foo\"]" } },
            ))
        }
        val result = simulation(mock())

        result shouldBe arrayOf("foo")
    }

    @Test
    fun `JsonValueArraySimulator returns NULL when JSON to parse is not an array`() {
        val simulation = context(subexpressionCallbacks) {
            JsonValueArraySimulator().simulateExpression(JsonValueArray(
                mock { whenever(subexpressionCallbacks.simulateExpression(it)).thenReturn { _ -> "\"foo\"" } },
            ))
        }
        val result = simulation(mock())

        result shouldBe null
    }

    @Test
    fun `JsonValueArraySimulator returns NULL for non scalar element in array`() {
        val simulation = context(subexpressionCallbacks) {
            JsonValueArraySimulator().simulateExpression(JsonValueArray(
                mock { whenever(subexpressionCallbacks.simulateExpression(it)).thenReturn { _ -> "[{ \"foo\": \"bar\" }]" } },
            ))
        }
        val result = simulation(mock())

        result shouldBe null
    }

    @Test
    fun `JsonValueArraySimulator parses array node selected by JSONPath as string`() {
        val simulation = context(subexpressionCallbacks) {
            JsonValueArraySimulator().simulateExpression(JsonValueArray(
                mock { whenever(subexpressionCallbacks.simulateExpression(it)).thenReturn { _ -> "{ \"foo\": [\"bar\"] }" } },
                mock { whenever(subexpressionCallbacks.simulateExpression(it)).thenReturn { _ -> "$.foo" } },
            ))
        }
        val result = simulation(mock())

        result shouldBe arrayOf("bar")
    }

    @Test
    fun `JsonValueArraySimulator can simulate JsonValue wrapping aggregations`() {
        val simulation = context(emptyList<Expression<BigQueryEngine, *>>(), subexpressionCallbacks) {
            JsonValueArraySimulator().simulateAggregation(JsonValueArray(
                mock { whenever(subexpressionCallbacks.simulateAggregation(it)).thenReturn { _ -> "{ \"foo\": [\"bar\"] }" } },
                mock { whenever(subexpressionCallbacks.simulateAggregation(it)).thenReturn { _ -> "$.foo" } },
            ))
        }
        val result = simulation(mock())

        result shouldBe arrayOf("bar")
    }
}
