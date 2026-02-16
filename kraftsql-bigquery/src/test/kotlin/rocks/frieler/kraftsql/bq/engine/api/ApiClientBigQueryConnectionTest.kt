package rocks.frieler.kraftsql.bq.engine.api

import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.Field
import com.google.cloud.bigquery.FieldList
import com.google.cloud.bigquery.StandardTableDefinition
import com.google.cloud.bigquery.TableInfo
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import rocks.frieler.kraftsql.bq.engine.Types
import rocks.frieler.kraftsql.ddl.CreateTable
import rocks.frieler.kraftsql.bq.objects.Table
import rocks.frieler.kraftsql.objects.Column
import rocks.frieler.kraftsql.objects.DataRow

class ApiClientBigQueryConnectionTest {
    private val bqClient = mock<BigQuery>()
    private val bqConnection = ApiClientBigQueryConnection(bqClient)

    @Test
    fun `ApiClientBigQueryConnection can execute CreateTable`() {
        val table = Table<DataRow>(null, "dataset", "table", listOf(
            Column("text", Types.STRING, nullable = false),
            Column("number", Types.INT64, nullable = true),
        ))

        bqConnection.execute(CreateTable(table))

        val createdTable = argumentCaptor<TableInfo>().run {
            verify(bqClient).create(capture())
            singleValue
        }
        createdTable.tableId.dataset shouldBe table.dataset
        createdTable.tableId.table shouldBe table.name
        createdTable.getDefinition<StandardTableDefinition>().schema!!.fields shouldContainExactly listOf(
            Field.newBuilder("text", Types.STRING.name).setMode(Field.Mode.REQUIRED).build(),
            Field.newBuilder("number", Types.INT64.name).setMode(Field.Mode.NULLABLE).build(),
        )
    }

    @Test
    fun `ApiClientBigQueryConnection can execute CreateTable with Array-of-Primitives-typed column`() {
        val table = Table<DataRow>(null, "dataset", "table", listOf(
            Column("array_of_strings", Types.ARRAY(Types.STRING), nullable = false),
        ))

        bqConnection.execute(CreateTable(table))

        val createdTable = argumentCaptor<TableInfo>().run {
            verify(bqClient).create(capture())
            singleValue
        }
        createdTable.tableId.dataset shouldBe table.dataset
        createdTable.tableId.table shouldBe table.name
        createdTable.getDefinition<StandardTableDefinition>().schema!!.fields shouldContainExactly listOf(
            Field.newBuilder("array_of_strings", Types.STRING.name).setMode(Field.Mode.REPEATED).build(),
        )
    }

    @Test
    fun `ApiClientBigQueryConnection can execute CreateTable with Array-of-Structs-typed column`() {
        val table = Table<DataRow>(null, "dataset", "table", listOf(
            Column("array_of_structs", Types.ARRAY(Types.STRUCT(listOf(Column("text", Types.STRING)))), nullable = false),
        ))

        bqConnection.execute(CreateTable(table))

        val createdTable = argumentCaptor<TableInfo>().run {
            verify(bqClient).create(capture())
            singleValue
        }
        createdTable.tableId.dataset shouldBe table.dataset
        createdTable.tableId.table shouldBe table.name
        createdTable.getDefinition<StandardTableDefinition>().schema!!.fields shouldContainExactly listOf(
            Field.newBuilder(
                "array_of_structs",
                Types.STRUCT(listOf(Column("text", Types.STRING))).name,
                FieldList.of(Field.newBuilder("text", Types.STRING.name).setMode(Field.Mode.NULLABLE).build()))
                .setMode(Field.Mode.REPEATED)
                .build(),
        )
    }
}
