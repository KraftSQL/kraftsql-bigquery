package rocks.frieler.kraftsql.bq.testing.engine

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.expressions.Replace
import rocks.frieler.kraftsql.expressions.Expression
import rocks.frieler.kraftsql.testing.engine.ExpressionSimulator

class ReplaceSimulatorTest {
    private val subexpressionCallbacks = mock<ExpressionSimulator.SubexpressionCallbacks<BigQueryEngine>>()

    @Test
    fun `ReplaceSimulator leaves NULL as is`() {
        val simulation = context(subexpressionCallbacks) {
            ReplaceSimulator().simulateExpression(Replace(
                mock { whenever(subexpressionCallbacks.simulateExpression(it)).thenReturn { _ -> null } },
                mock { whenever(subexpressionCallbacks.simulateExpression(it)).thenReturn { _ -> "World" } },
                mock { whenever(subexpressionCallbacks.simulateExpression(it)).thenReturn { _ -> "KraftSQL" } },
            ))
        }
        val result = simulation(mock())

        result shouldBe null
    }

    @Test
    fun `ReplaceSimulator replaces nothing with empty fromPattern`() {
        val simulation = context(subexpressionCallbacks) {
            ReplaceSimulator().simulateExpression(Replace(
                mock { whenever(subexpressionCallbacks.simulateExpression(it)).thenReturn { _ -> "Hello World!" } },
                mock { whenever(subexpressionCallbacks.simulateExpression(it)).thenReturn { _ -> "" } },
                mock { whenever(subexpressionCallbacks.simulateExpression(it)).thenReturn { _ -> "KraftSQL" } },
            ))
        }
        val result = simulation(mock())

        result shouldBe "Hello World!"
    }

    @Test
    fun `ReplaceSimulator replaces fromPattern with toPattern`() {
        val simulation = context(subexpressionCallbacks) {
            ReplaceSimulator().simulateExpression(Replace(
                mock { whenever(subexpressionCallbacks.simulateExpression(it)).thenReturn { _ -> "Hello World!" } },
                mock { whenever(subexpressionCallbacks.simulateExpression(it)).thenReturn { _ -> "World" } },
                mock { whenever(subexpressionCallbacks.simulateExpression(it)).thenReturn { _ -> "KraftSQL" } },
            ))
        }
        val result = simulation(mock())

        result shouldBe "Hello KraftSQL!"
    }

    @Test
    fun `ReplaceSimulator can simulate Replace wrapping Aggregations`() {
        val simulation = context(emptyList<Expression<BigQueryEngine, *>>(), subexpressionCallbacks) {
            ReplaceSimulator().simulateAggregation(Replace(
                mock { whenever(subexpressionCallbacks.simulateAggregation(it)).thenReturn { _ -> "Hello World!" } },
                mock { whenever(subexpressionCallbacks.simulateAggregation(it)).thenReturn { _ -> "World" } },
                mock { whenever(subexpressionCallbacks.simulateAggregation(it)).thenReturn { _ -> "KraftSQL" } },
            ))
        }
        val result = simulation(listOf(mock()))

        result shouldBe "Hello KraftSQL!"
    }
}
