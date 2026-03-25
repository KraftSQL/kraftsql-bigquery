package rocks.frieler.kraftsql.bq.testing.simulator.engine

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.objects.Table
import rocks.frieler.kraftsql.bq.objects.TemporaryTable
import rocks.frieler.kraftsql.objects.DataRow
import rocks.frieler.kraftsql.testing.simulator.engine.EngineState

class SessionStateTest {
    private val parentState = mock<EngineState<BigQueryEngine>>()
    private val sessionState = SessionState(parentState)

    @Test
    fun `containsTable() returns true for table in parent state`() {
        val existingTable = "existingTable"
        whenever(parentState.containsTable(existingTable)).thenReturn(true)

        val containsTable = sessionState.containsTable(existingTable)

        containsTable shouldBe true
    }

    @Test
    fun `containsTable() returns false for table neither existing in the session nor in parent state`() {
        val nonExistingTable = "nonExistingTable"
        whenever(parentState.containsTable(nonExistingTable)).thenReturn(false)

        val containsTable = sessionState.containsTable(nonExistingTable)

        containsTable shouldBe false
    }

    @Test
    fun `findTable() returns table from parent state`() {
        val table = mock<Table<BigQueryEngine>> {
            whenever(it.qualifiedName).thenReturn("table")
        }
        val tableContent = mock<MutableList<DataRow>>()
        whenever(parentState.findTable(table.qualifiedName)).thenReturn(Pair(table, tableContent))

        val foundTable = sessionState.findTable(table.qualifiedName)

        foundTable shouldBe Pair(table, tableContent)
    }

    @Test
    fun `findTable() returns null for table neither existing in the session nor in parent state`() {
        val nonExistingTable = "nonExistingTable"
        whenever(parentState.findTable(nonExistingTable)).thenReturn(null)

        val foundTable = sessionState.findTable(nonExistingTable)

        foundTable shouldBe null
    }

    @Test
    fun `addTable() adds regular table to parent state`() {
        val table = mock<Table<BigQueryEngine>> {
            whenever(it.qualifiedName).thenReturn("table")
        }

        sessionState.addTable(table)

        verify(parentState).addTable(table)
    }

    @Test
    fun `addTable() adds temporary table to the session`() {
        val tempTable = mock<TemporaryTable<BigQueryEngine>> {
            whenever(it.qualifiedName).thenReturn("temp_table")
        }

        sessionState.addTable(tempTable)

        verify(parentState, never()).addTable(tempTable)
        sessionState.getTable(tempTable.qualifiedName) shouldBe Pair(tempTable, mutableListOf())
    }

    @Test
    fun `removeTable() delegates removal of regular table to parent state`() {
        val table = mock<Table<BigQueryEngine>> {
            whenever(it.qualifiedName).thenReturn("table")
        }

        sessionState.removeTable(table)

        verify(parentState).removeTable(table)
    }

    @Test
    fun `removeTable() removes temporary table from the session`() {
        val tempTable = mock<TemporaryTable<BigQueryEngine>> {
            whenever(it.qualifiedName).thenReturn("temp_table")
        }

        sessionState.addTable(tempTable)
        check(sessionState.containsTable(tempTable.qualifiedName)) { "Temporary table must be present in the session" }
        sessionState.removeTable(tempTable)
        val containsTableAfterRemoval = sessionState.containsTable(tempTable.qualifiedName)

        verify(parentState, never()).removeTable(tempTable)
        containsTableAfterRemoval shouldBe false
    }

    @Test
    fun `writeTable() delegates writing of regular table to parent state`() {
        val table = mock<Table<BigQueryEngine>> {
            whenever(it.qualifiedName).thenReturn("table")
        }
        val data = listOf<DataRow>(mock(), mock())

        sessionState.writeTable(table, data)

        verify(parentState).writeTable(table, data)
    }

    @Test
    fun `writeTable() writes content of temporary table to the session`() {
        val tempTable = mock<TemporaryTable<BigQueryEngine>> {
            whenever(it.qualifiedName).thenReturn("temp_table")
        }
        val data = listOf<DataRow>(mock(), mock())

        sessionState.writeTable(tempTable, data)

        sessionState.getTable(tempTable.qualifiedName) shouldBe Pair(tempTable, data.toMutableList())
    }
}
