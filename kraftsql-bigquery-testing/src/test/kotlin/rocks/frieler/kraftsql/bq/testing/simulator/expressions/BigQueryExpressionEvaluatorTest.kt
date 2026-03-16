package rocks.frieler.kraftsql.bq.testing.simulator.expressions

import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import org.junit.jupiter.api.Test
import rocks.frieler.kraftsql.bq.expressions.ArrayConcat
import rocks.frieler.kraftsql.bq.expressions.ArrayLength
import rocks.frieler.kraftsql.bq.expressions.Constant
import rocks.frieler.kraftsql.bq.expressions.JsonValue
import rocks.frieler.kraftsql.bq.expressions.JsonValueArray
import rocks.frieler.kraftsql.bq.expressions.Replace
import rocks.frieler.kraftsql.bq.expressions.Struct
import rocks.frieler.kraftsql.bq.expressions.Timestamp
import rocks.frieler.kraftsql.bq.expressions.Unnest
import rocks.frieler.kraftsql.bq.objects.ConstantData
import rocks.frieler.kraftsql.expressions.Array
import rocks.frieler.kraftsql.expressions.ArrayElementReference
import rocks.frieler.kraftsql.objects.DataRow
import java.time.Instant

class BigQueryExpressionEvaluatorTest {
    @Test
    fun `BigQueryExpressionEvaluator can simulate BigQuery Constant`() {
        val constant = Constant(42)

        val simulation = BigQueryExpressionEvaluator.simulateExpression(constant)
        val result = simulation(DataRow())

        result shouldBe 42
    }

    @Test
    fun `BigQueryExpressionEvaluator can simulate BigQuery Struct`() {
        val struct = Struct<DataRow>(mapOf("number" to Constant(42)))

        val simulation = BigQueryExpressionEvaluator.simulateExpression(struct)
        val result = simulation(DataRow())

        result shouldBe DataRow("number" to 42)
    }

    @Test
    fun `BigQueryExpressionEvaluator can simulate BigQuery Replace`() {
        val replace = Replace(Constant("Hello World!"), Constant("World"), Constant("KraftSQL"))

        val simulation = BigQueryExpressionEvaluator.simulateExpression(replace)
        val result = simulation(DataRow())

        result shouldBe "Hello KraftSQL!"
    }

    @Test
    fun `BigQueryExpressionEvaluator can simulate BigQuery Timestamp`() {
        val timestamp = Timestamp(Constant("2008-12-25T15:30:00Z"))

        val simulation = BigQueryExpressionEvaluator.simulateExpression(timestamp)
        val result = simulation(DataRow())

        result shouldBe Instant.parse("2008-12-25T15:30:00Z")
    }

    @Test
    fun `GenericSimulatorConnection simulates a 0-based ArrayElementReference`() {
        val arrayElementReference = ArrayElementReference(Array.Companion(Constant(42)), Constant(0))

        val simulation = BigQueryExpressionEvaluator.simulateExpression(arrayElementReference)
        val result = simulation(DataRow())

        result shouldBe 42
    }

    @Test
    fun `GenericSimulatorConnection can simulate an ArrayLength expression`() {
        val arrayLength = ArrayLength(Array.Companion(Constant(1), Constant(2)))

        val simulation = BigQueryExpressionEvaluator.simulateExpression(arrayLength)
        val result = simulation(DataRow())

        result shouldBe 2L
    }

    @Test
    fun `BigQueryExpressionEvaluator can simulate BigQuery's ArrayConcat function`() {
        val arrayConcat = ArrayConcat(
            Array.Companion(Constant(1), Constant(2)),
            Array.Companion(Constant(3), Constant(4))
        )

        val simulation = BigQueryExpressionEvaluator.simulateExpression(arrayConcat)
        val result = simulation(DataRow())

        result shouldBe arrayOf(1, 2, 3, 4)
    }

    @Test
    fun `BigQueryExpressionEvaluator can simulate BigQuery's JsonValue function`() {
        val jsonValue = JsonValue(Constant("'foo'"))

        val simulation = BigQueryExpressionEvaluator.simulateExpression(jsonValue)
        val result = simulation(DataRow())

        result shouldBe "foo"
    }

    @Test
    fun `BigQueryExpressionEvaluator can simulate BigQuery's JsonValueArray function`() {
        val jsonValueArray = JsonValueArray(Constant("['foo']"))

        val simulation = BigQueryExpressionEvaluator.simulateExpression(jsonValueArray)
        val result = simulation(DataRow())

        result shouldBe arrayOf("foo")
    }

    @Test
    fun `BigQueryExpressionEvaluator can simulate Unnest`() {
        val unnest = Unnest(Array.Companion(Constant(1), Constant(2), Constant(3)))

        val simulation = BigQueryExpressionEvaluator.simulateExpression(unnest)
        val result = simulation(DataRow())

        result.shouldBeInstanceOf<ConstantData<Int>> {
            it.items shouldContainExactly listOf(1, 2, 3)
        }
    }
}
