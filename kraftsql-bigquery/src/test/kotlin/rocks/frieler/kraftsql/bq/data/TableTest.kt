package rocks.frieler.kraftsql.bq.data

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import rocks.frieler.kraftsql.data.DataRow
import rocks.frieler.kraftsql.engine.DataClassSerDe

class TableTest {
    @Test
    fun `Table has its project as database and its dataset as namespace while name is just name`() {
        val table = Table<DataRow>("proj", "dataset", "table_name", emptyList())

        table.project shouldBe "proj"
        table.database shouldBe table.project
        table.dataset shouldBe "dataset"
        table.namespace shouldBe table.dataset
        table.name shouldBe "table_name"
    }

    @Test
    fun `Table() derives the schema from the data class`() {
        data class Thing(val id: Int, val value: String?)

        val table = Table<Thing>("proj", "ds", "things")

        table.schema shouldBe DataClassSerDe.getSchemaFor(Thing::class)
    }

    @Test
    fun `Table() without database creates a Table in the Engine's default database`() {
        data class Thing(val id: Int, val value: String?)

        val table = Table<Thing>("ds", "things")

        table.project shouldBe null
    }
}
