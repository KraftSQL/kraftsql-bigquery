package rocks.frieler.kraftsql.bq.engine

import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.Field
import com.google.cloud.bigquery.FieldList
import com.google.cloud.bigquery.FieldValue
import com.google.cloud.bigquery.FieldValueList
import com.google.cloud.bigquery.TableResult
import io.kotest.matchers.collections.shouldContainExactly
import io.mockk.every
import io.mockk.mockk
import org.junit.jupiter.api.Test
import rocks.frieler.kraftsql.data.Column
import rocks.frieler.kraftsql.data.ConstantData
import rocks.frieler.kraftsql.data.Data
import rocks.frieler.kraftsql.data.DataRow
import rocks.frieler.kraftsql.data.Type
import rocks.frieler.kraftsql.engine.DataClassSerDe

class BigQueryEngineTest {
    private val bigQueryClient = mockk<BigQuery>()
    private val sqlGenerator = mockk<BigQuerySqlGenerator>()
    private val engine = BigQueryEngine(
        bigquery = bigQueryClient,
        sqlGenerator = sqlGenerator,
    )

    @Test
    fun `collect() can collect ConstantData of DataRows as DataRows`() {
        val items = listOf(DataRow("id" to 1), DataRow("id" to 2))
        val data = mockk<ConstantData<DataRow>> {
            every { this@mockk.items } returns items
            every { schema } returns listOf(Column("id", Type.INTEGER))
        }

        val query = "<SQL statement for $data>"
        every { sqlGenerator.makeSQL(data) } returns query
        every { bigQueryClient.query(match { it.query == query }) } returns mockTableResult(data)

        val collectedData = engine.collect(data, DataRow::class)

        collectedData shouldContainExactly items
    }

    @Test
    fun `collect() can collect ConstantData of DataRows as data-class instances`() {
        data class Answer(val answer: Int)
        val item = DataRow("answer" to 42)
        @Suppress("UNCHECKED_CAST") val data = mockk<ConstantData<DataRow>> {
            every { this@mockk.items } returns listOf(item)
            every { schema } returns listOf(Column("answer", Type.INTEGER))
        } as Data<Answer>

        val query = "<SQL statement for $data>"
        every { sqlGenerator.makeSQL(data) } returns query
        every { bigQueryClient.query(match { it.query == query }) } returns mockTableResult(data as ConstantData<Answer>)

        val collectedData = engine.collect(data, Answer::class)

        collectedData shouldContainExactly listOf(Answer(42))
    }

    @Test
    fun `collect() can collect ConstantData of data-class instances as DataRows`() {
        data class Answer(val answer: Int)
        val item = Answer(42)
        @Suppress("UNCHECKED_CAST") val data = mockk<ConstantData<Answer>> {
            every { this@mockk.items } returns listOf(item)
            every { schema } returns listOf(Column("answer", Type.INTEGER))
        } as Data<DataRow>

        val query = "<SQL statement for $data>"
        every { sqlGenerator.makeSQL(data) } returns query
        every { bigQueryClient.query(match { it.query == query }) } returns mockTableResult(data as ConstantData<DataRow>)

        val collectedData = engine.collect(data, DataRow::class)

        collectedData shouldContainExactly listOf(DataRow("answer" to 42))
    }

    @Test
    fun `collect() can collect values of all supported types`() {
        val items = listOf(DataRow(
            "int" to 1,
            "string" to "KraftSQL",
        ))
        val data = mockk<ConstantData<DataRow>> {
            every { this@mockk.items } returns items
            every { schema } returns listOf(
                Column("int", Type.INTEGER),
                Column("string", Type.STRING),
            )
        }

        val query = "<SQL statement for $data>"
        every { sqlGenerator.makeSQL(data) } returns query
        every { bigQueryClient.query(match { it.query == query }) } returns mockTableResult(data)

        val collectedData = engine.collect(data, DataRow::class)

        collectedData shouldContainExactly items
    }

    @Test
    fun `collect() can collect nulls and values from nullable columns`() {
        val items = listOf(
            DataRow("nullable_int" to 1, "nullable_string" to "KraftSQL"),
            DataRow("nullable_int" to null, "nullable_string" to null),
        )
        val data = mockk<ConstantData<DataRow>> {
            every { this@mockk.items } returns items
            every { schema } returns listOf(
                Column("nullable_int", Type.NULLABLE_INTEGER),
                Column("nullable_string", Type.NULLABLE_STRING),
            )
        }

        val query = "<SQL statement for $data>"
        every { sqlGenerator.makeSQL(data) } returns query
        every { bigQueryClient.query(match { it.query == query }) } returns mockTableResult(data)

        val collectedData = engine.collect(data, DataRow::class)

        collectedData shouldContainExactly items
    }

    @Test
    fun `collect() can collect special Strings`() {
        val items = listOf(DataRow(
            "string_with_quotes" to "it's \"quoted\"",
            "empty_string" to "",
        ))
        val data = mockk<ConstantData<DataRow>> {
            every { this@mockk.items } returns items
            every { schema } returns listOf(
                Column("string_with_quotes", Type.STRING),
                Column("empty_string", Type.STRING),
            )
        }

        val query = "<SQL statement for $data>"
        every { sqlGenerator.makeSQL(data) } returns query
        every { bigQueryClient.query(match { it.query == query }) } returns mockTableResult(data)

        val collectedData = engine.collect(data, DataRow::class)

        collectedData shouldContainExactly items
    }

    private fun mockTableResult(data: ConstantData<*>): TableResult = mockk {
        val fields = FieldList.of(data.schema.map { column -> Field.of(column.name, column.type.native) })
        every { values } returns data.items.map { item ->
            val row = item as? DataRow ?: DataClassSerDe.serialize(item)
            FieldValueList.of(fields.map { field -> FieldValue.of(FieldValue.Attribute.PRIMITIVE, row[field.name]?.toString()) }, fields)
        }
    }
}
