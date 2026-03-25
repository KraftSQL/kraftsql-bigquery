package rocks.frieler.kraftsql.bq.testing.simulator.expressions

import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import io.kotest.matchers.types.shouldBeInstanceOf
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.expressions.Unnest
import rocks.frieler.kraftsql.bq.objects.ConstantData
import rocks.frieler.kraftsql.expressions.Expression
import rocks.frieler.kraftsql.objects.DataRow
import rocks.frieler.kraftsql.testing.simulator.expressions.ExpressionSimulator

class UnnestSimulatorTest {
    private val subexpressionCallbacks = mock<ExpressionSimulator.SubexpressionCallbacks<BigQueryEngine>>()
    private val unnestSimulator = UnnestSimulator<Any>()

    @Test
    fun `UnnestSimulator expands Array to ConstantData containing its content`() {
        val unnest = Unnest(mock<Expression<BigQueryEngine, Array<Any>>>())
        val element1 = Any()
        val element2 = Any()
        whenever(subexpressionCallbacks.simulateExpression(unnest.arrayExpression)).thenReturn({ _ -> arrayOf(element1, element2) })

        val simulatedUnnest = context(subexpressionCallbacks) { unnestSimulator.simulateExpression(unnest) }
        val result = simulatedUnnest(DataRow())

        result.shouldBeInstanceOf<ConstantData<Any>> {
            it.items.toList() shouldContainExactlyInAnyOrder listOf(element1, element2)
        }
    }
    @Test
    fun `UnnestSimulator expands empty Array to empty ConstantData`() {
        val unnest = Unnest(mock<Expression<BigQueryEngine, Array<Any>>>())
        whenever(subexpressionCallbacks.simulateExpression(unnest.arrayExpression)).thenReturn({ _ -> emptyArray<Any>() })

        val simulatedUnnest = context(subexpressionCallbacks) { unnestSimulator.simulateExpression(unnest) }
        val result = simulatedUnnest(DataRow())

        result.shouldBeInstanceOf<ConstantData<Any>> {
            it.items.shouldBeEmpty()
        }
    }

    @Test
    fun `UnnestSimulator expands NULL Array to empty ConstantData`() {
        val unnest = Unnest(mock<Expression<BigQueryEngine, Array<Any>>>())
        whenever(subexpressionCallbacks.simulateExpression(unnest.arrayExpression)).thenReturn({ _ -> null })

        val simulatedUnnest = context(subexpressionCallbacks) { unnestSimulator.simulateExpression(unnest) }
        val result = simulatedUnnest(DataRow())

        result.shouldBeInstanceOf<ConstantData<Any>> {
            it.items.shouldBeEmpty()
        }
    }

    @Test
    fun `UnnestSimulator can simulate Unnest wrapping an aggregation`() {
        val unnest = Unnest(mock<Expression<BigQueryEngine, Array<Any>>>())
        val element1 = Any()
        val element2 = Any()
        whenever(context(emptyList<Expression<BigQueryEngine, *>>()) { subexpressionCallbacks.simulateAggregation(unnest.arrayExpression) })
            .thenReturn({ _ -> arrayOf(element1, element2) })

        val simulatedUnnest = context(emptyList<Expression<BigQueryEngine, *>>(), subexpressionCallbacks,) {
            unnestSimulator.simulateAggregation(unnest)
        }
        val result = simulatedUnnest(listOf(DataRow()))

        result.shouldBeInstanceOf<ConstantData<Any>> {
            it.items.toList() shouldContainExactlyInAnyOrder listOf(element1, element2)
        }
    }
}
