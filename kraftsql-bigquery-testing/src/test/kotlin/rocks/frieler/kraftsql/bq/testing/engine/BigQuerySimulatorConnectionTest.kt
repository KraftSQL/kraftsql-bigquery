package rocks.frieler.kraftsql.bq.testing.engine

import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import rocks.frieler.kraftsql.bq.dql.Select
import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.expressions.Constant
import rocks.frieler.kraftsql.bq.expressions.JsonValue
import rocks.frieler.kraftsql.bq.expressions.JsonValueArray
import rocks.frieler.kraftsql.bq.expressions.Replace
import rocks.frieler.kraftsql.bq.expressions.Struct
import rocks.frieler.kraftsql.bq.expressions.Timestamp
import rocks.frieler.kraftsql.bq.objects.ConstantData
import rocks.frieler.kraftsql.dql.Projection
import rocks.frieler.kraftsql.dql.QuerySource
import rocks.frieler.kraftsql.expressions.Column
import rocks.frieler.kraftsql.objects.DataRow
import java.time.Instant

class BigQuerySimulatorConnectionTest {
    private val connection = BigQuerySimulatorConnection()

    @Test
    fun `BigQuerySimulatorConnection can simulate SELECT from constant data`() {
        val result = connection.execute(
            Select(
                source = QuerySource(ConstantData(DataRow("name" to "foo"))),
                columns = listOf(Projection(Column<BigQueryEngine, String>("name"))),
            ), DataRow::class
        )

        result shouldContainExactly listOf(DataRow("name" to "foo"))
    }

    @Test
    fun `BigQuerySimulatorConnection can simulate BigQuery Constant`() {
        val result = connection.execute(
            Select(
                source = QuerySource(ConstantData(DataRow())),
                columns = listOf(Projection(Constant(42), "const")),
            ), DataRow::class
        )

        result.single()["const"] shouldBe 42
    }

    @Test
    fun `BigQuerySimulatorConnection can simulate BigQuery Struct`() {
        val result = connection.execute(
            Select(
                source = QuerySource(ConstantData(DataRow())),
                columns = listOf(Projection(Struct<DataRow>(mapOf("number" to Constant(42))), "struct")),
            ), DataRow::class
        )

        result.single()["struct"] shouldBe DataRow("number" to 42)
    }

    @Test
    fun `BigQuerySimulatorConnection can simulate BigQuery Replace`() {
        val result = connection.execute(
            Select(
                source = QuerySource(ConstantData(DataRow())),
                columns = listOf(Projection(Replace(Constant("Hello World!"), Constant("World"), Constant("KraftSQL")), "greeting")),
            ), DataRow::class
        )

        result.single()["greeting"] shouldBe "Hello KraftSQL!"
    }

    @Test
    fun `BigQuerySimulatorConnection can simulate BigQuery Timestamp`() {
        val result = connection.execute(
            Select(
                source = QuerySource(ConstantData(DataRow())),
                columns = listOf(Projection(Timestamp(Constant("2008-12-25T15:30:00Z")), "timestamp")),
            ), DataRow::class
        )

        result.single()["timestamp"] shouldBe Instant.parse("2008-12-25T15:30:00Z")
    }

    @Test
    fun `BigQuerySimulatorConnection can simulate BigQuery's JsonValue function`() {
        val result = connection.execute(
            Select(
                source = QuerySource(ConstantData(DataRow())),
                columns = listOf(Projection(JsonValue(Constant("'foo'")), "parsed_value")),
            ), DataRow::class
        )

        result.single()["parsed_value"] shouldBe "foo"
    }

    @Test
    fun `BigQuerySimulatorConnection can simulate BigQuery's JsonValueArray function`() {
        val result = connection.execute(
            Select(
                source = QuerySource(ConstantData(DataRow())),
                columns = listOf(Projection(JsonValueArray(Constant("['foo']")), "parsed_value")),
            ), DataRow::class
        )

        result.single()["parsed_value"] shouldBe arrayOf("foo")
    }
}
