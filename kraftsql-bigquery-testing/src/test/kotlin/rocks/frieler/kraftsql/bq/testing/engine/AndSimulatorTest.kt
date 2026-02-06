package rocks.frieler.kraftsql.bq.testing.engine

import io.kotest.matchers.shouldBe
import org.junit.jupiter.params.ParameterizedTest
import org.junit.jupiter.params.provider.CsvSource
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.expressions.And
import rocks.frieler.kraftsql.expressions.Expression
import rocks.frieler.kraftsql.objects.DataRow
import rocks.frieler.kraftsql.testing.engine.ExpressionSimulator

class AndSimulatorTest {
    private val subexpressionCallbacks = mock<ExpressionSimulator.SubexpressionCallbacks<BigQueryEngine>>()

    @ParameterizedTest
    @CsvSource(value = [
        "true, true, true",
        "true, false, false",
        "true, NULL, NULL", //
        "false, true, false",
        "false, false, false",
        "false, NULL, false",
        "NULL, true, NULL", //
        "NULL, false, false",
        "NULL, NULL, NULL", //
    ], nullValues = ["NULL"])
    fun `AndSimulator can simulate AND-operator to combine two expressions`(left: Boolean?, right: Boolean?, expectedResult: Boolean?) {
        val row = mock<DataRow>()
        val leftHandSide = mock<Expression<BigQueryEngine, Boolean?>>().also { whenever(subexpressionCallbacks.simulateExpression(it)).thenReturn { _ -> left} }
        val rightHandSide = mock<Expression<BigQueryEngine, Boolean?>>().also { whenever(subexpressionCallbacks.simulateExpression(it)).thenReturn { _ -> right} }

        val simulation = context(subexpressionCallbacks) {
            AndSimulator().simulateExpression(And(leftHandSide, rightHandSide))
        }
        val result = simulation(row)

        result shouldBe expectedResult
    }
}
