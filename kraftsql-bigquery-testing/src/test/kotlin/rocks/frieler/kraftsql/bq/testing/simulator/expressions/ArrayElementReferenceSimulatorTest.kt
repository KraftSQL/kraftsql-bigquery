package rocks.frieler.kraftsql.bq.testing.simulator.expressions

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.expressions.ArrayElementReference
import rocks.frieler.kraftsql.expressions.Expression
import rocks.frieler.kraftsql.testing.simulator.expressions.ExpressionSimulator

class ArrayElementReferenceSimulatorTest {
    private val arrayElementReferenceSimulator = ArrayElementReferenceSimulator<Any?>()

    private val subexpressionCallbacks = mock<ExpressionSimulator.SubexpressionCallbacks<BigQueryEngine>>()

    @Test
    fun `ArrayElementReferenceSimulator can simulate ArrayElementReference`() {
        val arrayExpression = mock<Expression<BigQueryEngine, Array<Any?>?>>()
        whenever(subexpressionCallbacks.simulateExpression(arrayExpression)).thenReturn { _ -> arrayOf("a", "b", "c") }
        val indexExpression = mock<Expression<BigQueryEngine, Int>>()
        whenever(subexpressionCallbacks.simulateExpression(indexExpression)).thenReturn { _ -> 1 }

        val simulatedArrayElementReference = context(subexpressionCallbacks) {
            arrayElementReferenceSimulator.simulateExpression(ArrayElementReference(arrayExpression, indexExpression))
        }
        val result = simulatedArrayElementReference(mock())

        result shouldBe "b"
    }
}
