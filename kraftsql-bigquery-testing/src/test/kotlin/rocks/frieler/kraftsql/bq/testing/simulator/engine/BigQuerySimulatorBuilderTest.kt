package rocks.frieler.kraftsql.bq.testing.simulator.engine

import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.shouldBeInstanceOf
import io.mockk.mockk
import org.junit.jupiter.api.Test
import rocks.frieler.kraftsql.bq.dql.Select
import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
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
import rocks.frieler.kraftsql.dql.Projection
import rocks.frieler.kraftsql.dql.QuerySource
import rocks.frieler.kraftsql.engine.Connection
import rocks.frieler.kraftsql.expressions.Array
import rocks.frieler.kraftsql.expressions.ArrayElementReference
import rocks.frieler.kraftsql.expressions.Expression
import rocks.frieler.kraftsql.objects.DataRow
import java.time.Instant

class BigQuerySimulatorBuilderTest {
    private val builder = BigQuerySimulatorBuilder()
    private val simulator = builder.build()

    private fun BigQuerySimulator.evaluate(expression: Expression<BigQueryEngine, *>): Any? =
        Select<DataRow>(
            source = QuerySource(ConstantData(DataRow())),
            columns = listOf(Projection(expression))
        ).let {
            context(mockk<Connection<BigQueryEngine>>()) { this.execute(it, DataRow::class) }
        }.single().entries.single().second

    @Test
    fun `BigQuerySimulator can simulate BigQuery Constant`() {
        val constant = Constant(42)

        val result = simulator.evaluate(constant)

        result shouldBe 42
    }

    @Test
    fun `BigQuerySimulator can simulate BigQuery Struct`() {
        val struct = Struct<DataRow>(mapOf("number" to Constant(42)))

        val result = simulator.evaluate(struct)

        result shouldBe DataRow("number" to 42)
    }

    @Test
    fun `BigQuerySimulator can simulate BigQuery Replace`() {
        val replace = Replace(Constant("Hello World!"), Constant("World"), Constant("KraftSQL"))

        val result = simulator.evaluate(replace)

        result shouldBe "Hello KraftSQL!"
    }

    @Test
    fun `BigQuerySimulator can simulate BigQuery Timestamp`() {
        val timestamp = Timestamp(Constant("2008-12-25T15:30:00Z"))

        val result = simulator.evaluate(timestamp)

        result shouldBe Instant.parse("2008-12-25T15:30:00Z")
    }

    @Test
    fun `BigQuerySimulator simulates a 0-based ArrayElementReference`() {
        val arrayElementReference = ArrayElementReference(Array.Companion(Constant(42)), Constant(0))

        val result = simulator.evaluate(arrayElementReference)

        result shouldBe 42
    }

    @Test
    fun `BigQuerySimulator can simulate an ArrayLength expression`() {
        val arrayLength = ArrayLength(Array.Companion(Constant(1), Constant(2)))

        val result = simulator.evaluate(arrayLength)

        result shouldBe 2L
    }

    @Test
    fun `BigQuerySimulator can simulate BigQuery's ArrayConcat function`() {
        val arrayConcat = ArrayConcat(
            Array.Companion(Constant(1), Constant(2)),
            Array.Companion(Constant(3), Constant(4))
        )

        val result = simulator.evaluate(arrayConcat)

        result shouldBe arrayOf(1, 2, 3, 4)
    }

    @Test
    fun `BigQuerySimulator can simulate BigQuery's JsonValue function`() {
        val jsonValue = JsonValue(Constant("'foo'"))

        val result = simulator.evaluate(jsonValue)

        result shouldBe "foo"
    }

    @Test
    fun `BigQuerySimulator can simulate BigQuery's JsonValueArray function`() {
        val jsonValueArray = JsonValueArray(Constant("['foo']"))

        val result = simulator.evaluate(jsonValueArray)

        result shouldBe arrayOf("foo")
    }

    @Test
    fun `BigQuerySimulator can simulate Unnest`() {
        val unnest = Unnest(Array.Companion(Constant(1), Constant(2), Constant(3)))

        val result = simulator.evaluate(unnest)

        result.shouldBeInstanceOf<ConstantData<Int>> {
            it.items shouldContainExactly listOf(1, 2, 3)
        }
    }
}
