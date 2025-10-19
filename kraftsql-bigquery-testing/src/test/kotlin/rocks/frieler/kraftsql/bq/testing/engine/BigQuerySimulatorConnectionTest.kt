package rocks.frieler.kraftsql.bq.testing.engine

import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import rocks.frieler.kraftsql.bq.dql.Select
import rocks.frieler.kraftsql.bq.expressions.Constant
import rocks.frieler.kraftsql.bq.expressions.Replace
import rocks.frieler.kraftsql.bq.expressions.Struct
import rocks.frieler.kraftsql.bq.objects.ConstantData
import rocks.frieler.kraftsql.dql.Projection
import rocks.frieler.kraftsql.dql.QuerySource
import rocks.frieler.kraftsql.expressions.Column
import rocks.frieler.kraftsql.objects.DataRow

class BigQuerySimulatorConnectionTest {
    private val connection = BigQuerySimulatorConnection()

    @Test
    fun `BigQuerySimulatorConnection can simulate SELECT from constant data`() {
        val result = connection.execute(
            Select(
                source = QuerySource(ConstantData(DataRow(mapOf("name" to "foo")))),
                columns = listOf(Projection(Column("name"))),
            ), DataRow::class
        )

        result shouldContainExactly listOf(DataRow(mapOf("name" to "foo")))
    }

    @Test
    fun `BigQuerySimulatorConnection can simulate BigQuery Constant`() {
        val result = connection.execute(
            Select(
                source = QuerySource(ConstantData(DataRow(emptyMap()))),
                columns = listOf(Projection(Constant(42), "const")),
            ), DataRow::class
        )

        result.single()["const"] shouldBe 42
    }

    @Test
    fun `BigQuerySimulatorConnection can simulate BigQuery Struct`() {
        val result = connection.execute(
            Select(
                source = QuerySource(ConstantData(DataRow(emptyMap()))),
                columns = listOf(Projection(Struct(mapOf("number" to Constant(42))), "struct")),
            ), DataRow::class
        )

        result.single()["struct"] shouldBe DataRow(mapOf("number" to 42))
    }

    @Test
    fun `BigQuerySimulatorConnection can simulate BigQuery Replace`() {
        val result = connection.execute(
            Select(
                source = QuerySource(ConstantData(DataRow(emptyMap()))),
                columns = listOf(Projection(Replace(Constant("Hello World!"), Constant("World"), Constant("KraftSQL")), "greeting")),
            ), DataRow::class
        )

        result.single()["greeting"] shouldBe "Hello KraftSQL!"
    }
}
