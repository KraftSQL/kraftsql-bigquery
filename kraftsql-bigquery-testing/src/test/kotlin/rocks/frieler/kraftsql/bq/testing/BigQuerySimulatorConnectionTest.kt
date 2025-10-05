package rocks.frieler.kraftsql.bq.testing

import io.kotest.matchers.collections.shouldContainExactly
import org.junit.jupiter.api.Test
import rocks.frieler.kraftsql.bq.dql.Select
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
}
