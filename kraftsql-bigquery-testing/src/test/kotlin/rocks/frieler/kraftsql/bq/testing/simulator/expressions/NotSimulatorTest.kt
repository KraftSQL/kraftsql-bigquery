package rocks.frieler.kraftsql.bq.testing.simulator.expressions

import io.kotest.matchers.shouldBe
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.expressions.Expression
import rocks.frieler.kraftsql.expressions.Not
import rocks.frieler.kraftsql.objects.DataRow
import rocks.frieler.kraftsql.testing.simulator.expressions.ExpressionSimulator

class NotSimulatorTest {
    private val subexpressionCallbacks = mock<ExpressionSimulator.SubexpressionCallbacks<BigQueryEngine>>()

    @ParameterizedTest
    @CsvSource(value = [
        "true, false",
        "false, true",
        "NULL, NULL",
    ], nullValues = ["NULL"])
    fun `NotSimulator can simulate NOT-operator to negate an expression`(value: Boolean?, expectedResult: Boolean?) {
        val row = mock<DataRow>()
        val innerExpression = mock<Expression<BigQueryEngine, Boolean?>>().also { whenever(subexpressionCallbacks.simulateExpression(it)).thenReturn { _ -> value} }

        val simulation = context(subexpressionCallbacks) {
            NotSimulator().simulateExpression(Not(innerExpression))
        }
        val result = simulation(row)

        result shouldBe expectedResult
    }
}
