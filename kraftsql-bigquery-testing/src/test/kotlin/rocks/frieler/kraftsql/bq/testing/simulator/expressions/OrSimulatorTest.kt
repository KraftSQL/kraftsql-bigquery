package rocks.frieler.kraftsql.bq.testing.simulator.expressions

import io.kotest.matchers.shouldBe
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.expressions.Expression
import rocks.frieler.kraftsql.expressions.Or
import rocks.frieler.kraftsql.objects.DataRow
import rocks.frieler.kraftsql.testing.simulator.expressions.ExpressionSimulator

class OrSimulatorTest {
    private val subexpressionCallbacks = mock<ExpressionSimulator.SubexpressionCallbacks<BigQueryEngine>>()

    @ParameterizedTest
    @CsvSource(value = [
        "true, true, true",
        "true, false, true",
        "true, NULL, true",
        "false, true, true",
        "false, false, false",
        "false, NULL, NULL",
        "NULL, true, true",
        "NULL, false, NULL",
        "NULL, NULL, NULL",
    ], nullValues = ["NULL"])
    fun `OrSimulator can simulate OR-operator to combine two expressions`(left: Boolean?, right: Boolean?, expectedResult: Boolean?) {
        val row = mock<DataRow>()
        val leftHandSide = mock<Expression<BigQueryEngine, Boolean?>>().also { whenever(subexpressionCallbacks.simulateExpression(it)).thenReturn { _ -> left} }
        val rightHandSide = mock<Expression<BigQueryEngine, Boolean?>>().also { whenever(subexpressionCallbacks.simulateExpression(it)).thenReturn { _ -> right} }

        val simulation = context(subexpressionCallbacks) {
            OrSimulator().simulateExpression(Or(leftHandSide, rightHandSide))
        }
        val result = simulation(row)

        result shouldBe expectedResult
    }
}
