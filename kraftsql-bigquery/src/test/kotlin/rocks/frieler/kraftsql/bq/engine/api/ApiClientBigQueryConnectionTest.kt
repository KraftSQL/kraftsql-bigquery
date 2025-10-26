package rocks.frieler.kraftsql.bq.engine.api

import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.Field
import com.google.cloud.bigquery.StandardTableDefinition
import com.google.cloud.bigquery.TableInfo
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.engine.Types
import rocks.frieler.kraftsql.ddl.CreateTable
import rocks.frieler.kraftsql.bq.objects.Table
import rocks.frieler.kraftsql.objects.Column

class ApiClientBigQueryConnectionTest {
    private val bqClient = mock<BigQuery>()
    private val bqConnection = ApiClientBigQueryConnection(bqClient)

    @Test
    fun `ApiClientBigQueryConnection can execute CreateTable`() {
        val table = mock<Table<*>> {
            whenever(it.dataset).thenReturn("dataset")
            whenever(it.name).thenReturn("table")
            val stringColumn = mock<Column<BigQueryEngine>> {
                whenever(it.name).thenReturn("text")
                whenever(it.type).thenReturn(Types.STRING)
                whenever(it.nullable).thenReturn(false)
            }
            val optionalInt64Column = mock<Column<BigQueryEngine>> {
                whenever(it.name).thenReturn("number")
                whenever(it.type).thenReturn(Types.INT64)
                whenever(it.nullable).thenReturn(true)
            }
            whenever(it.columns).thenReturn(listOf(stringColumn, optionalInt64Column))
        }

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
}
