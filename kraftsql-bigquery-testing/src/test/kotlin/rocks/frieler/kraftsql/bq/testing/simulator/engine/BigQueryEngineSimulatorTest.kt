package rocks.frieler.kraftsql.bq.testing.simulator.engine

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import io.kotest.matchers.shouldNotBe
import io.kotest.matchers.types.shouldBeInstanceOf
import org.junit.jupiter.api.Test
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.clearInvocations
import org.mockito.kotlin.eq
import org.mockito.kotlin.mock
import org.mockito.kotlin.never
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import rocks.frieler.kraftsql.bq.dml.LoadData
import rocks.frieler.kraftsql.bq.dql.Select
import rocks.frieler.kraftsql.bq.engine.BigQueryConnection
import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.objects.FileSource
import rocks.frieler.kraftsql.bq.objects.Table
import rocks.frieler.kraftsql.bq.objects.TemporaryTable
import rocks.frieler.kraftsql.bq.testing.simulator.expressions.BigQueryExpressionEvaluator
import rocks.frieler.kraftsql.ddl.CreateTable
import rocks.frieler.kraftsql.ddl.DropTable
import rocks.frieler.kraftsql.dml.BeginTransaction
import rocks.frieler.kraftsql.engine.Connection
import rocks.frieler.kraftsql.objects.DataRow
import rocks.frieler.kraftsql.testing.simulator.engine.EngineState
import rocks.frieler.kraftsql.testing.simulator.engine.GenericQueryEvaluator
import rocks.frieler.kraftsql.testing.simulator.engine.TransactionStateOverlay

class BigQueryEngineSimulatorTest {
    val persistentState = mock<EngineState<BigQueryEngine>>()
    val queryEvaluator = mock<GenericQueryEvaluator<BigQueryEngine>> {
        whenever(it.expressionEvaluatorForChecking).thenReturn(BigQueryExpressionEvaluator)
    }
    private val bigQueryEngineSimulator = BigQueryEngineSimulator(
        persistentState = persistentState,
        queryEvaluator = queryEvaluator,
    )

    private val connection = mock<BigQueryConnection>()

    @Test
    fun `execute Select without session mode evaluates query on persistent state`() {
        whenever(connection.sessionMode).thenReturn(false)
        val select = mock<Select<Any>>()
        val expectedResult = listOf<DataRow>(mock(), mock())
        whenever(context(persistentState) { queryEvaluator.selectRows(select) })
            .thenReturn(expectedResult)

        val result = context(connection) {
            bigQueryEngineSimulator.execute(select, Any::class)
        }

        result shouldBe expectedResult
    }

    @Test
    fun `execute Select in session mode evaluates query on SessionState`() {
        whenever(connection.sessionMode).thenReturn(true)
        val select = mock<Select<Any>>()
        val expectedResult = listOf<DataRow>(mock(), mock())
        whenever(context(any<SessionState>()) { queryEvaluator.selectRows(eq(select), eq(null)) })
            .thenReturn(expectedResult)

        val result = context(connection) {
            bigQueryEngineSimulator.execute(select, Any::class)
        }

        result shouldBe expectedResult
    }

    @Test
    fun `execute CreateTable creates table in persistent state`() {
        val table = mock<Table<*>>()
        val createTable = mock<CreateTable<BigQueryEngine>> {
            whenever(it.table).thenReturn(table)
        }

        context(connection) { bigQueryEngineSimulator.execute(createTable) }

        verify(persistentState).addTable(table)
    }

    @Test
    fun `execute CreateTable cannot create temporary table`() {
        val table = mock<TemporaryTable<*>>()
        val createTable = mock<CreateTable<BigQueryEngine>> {
            whenever(it.table).thenReturn(table)
        }

        shouldThrow<UnsupportedOperationException> {
            context(connection) { bigQueryEngineSimulator.execute(createTable) }
        }
    }

    @Test
    fun `execute DropTable drops table from persistent state`() {
        val table = mock<Table<*>> {
            whenever(it.qualifiedName).thenReturn("table")
        }
        whenever(persistentState.containsTable(table.qualifiedName)).thenReturn(true)
        val dropTable = mock<DropTable<BigQueryEngine>> {
            whenever(it.table).thenReturn(table)
        }

        context(connection) { bigQueryEngineSimulator.execute(dropTable) }

        verify(persistentState).removeTable(table)
    }

    @Test
    fun `execute DropTable cannot create temporary table`() {
        val table = mock<TemporaryTable<*>>()
        val dropTable = mock<DropTable<BigQueryEngine>> {
            whenever(it.table).thenReturn(table)
        }

        shouldThrow<UnsupportedOperationException> {
            context(connection) { bigQueryEngineSimulator.execute(dropTable) }
        }
    }

    @Test
    fun `execute LoadData cannot load into a temporary table without session`() {
        whenever(connection.sessionMode).thenReturn(false)
        val tempTable = mock<TemporaryTable<*>>()
        val loadData = mock<LoadData> {
            whenever(it.fileSource).thenReturn(FileSource(emptyList(), "CSV"))
            whenever(it.columns).thenReturn(listOf(mock(), mock()))
            whenever(it.table).thenReturn(tempTable)
        }

        shouldThrow<IllegalStateException> {
            context(connection) { bigQueryEngineSimulator.execute(loadData) }
        }
    }

    @Test
    fun `execute BeginTransaction in session mode can begin transaction`() {
        whenever(connection.sessionMode).thenReturn(true)

        context(connection) {
            bigQueryEngineSimulator.execute(mock<BeginTransaction<BigQueryEngine>>())
        }
    }

    @Test
    fun `execute BeginTransaction requires session mode`() {
        whenever(connection.sessionMode).thenReturn(false)

        context(connection) {
            shouldThrow<IllegalStateException> {
                bigQueryEngineSimulator.execute(mock<BeginTransaction<BigQueryEngine>>())
            }
        }
    }

    @Test
    fun `execute BeginTransaction rejects nested transaction`() {
        whenever(connection.sessionMode).thenReturn(true)

        context(connection) {
            bigQueryEngineSimulator.execute(mock<BeginTransaction<BigQueryEngine>>())
            shouldThrow<IllegalStateException> {
                bigQueryEngineSimulator.execute(mock<BeginTransaction<BigQueryEngine>>())
            }
        }
    }

    @Test
    fun `abortSession ends the current session`() {
        whenever(connection.sessionMode).thenReturn(true)

        val sessions = context(connection) {
            val firstSession = captureTopState() as SessionState
            bigQueryEngineSimulator.abortSession()
            val secondSession = captureTopState() as SessionState
            firstSession to secondSession
        }

        sessions.second shouldNotBe sessions.first
    }

    @Test
    fun `abortSession rolls back running transaction`() {
        whenever(connection.sessionMode).thenReturn(true)

        val (firstState, secondState) = context(connection) {
            bigQueryEngineSimulator.execute(mock<BeginTransaction<BigQueryEngine>>())
            val transaction = captureTopState()
            transaction.writeTable(mock(), emptyList())
            bigQueryEngineSimulator.abortSession()
            val newSession = captureTopState()
            transaction to newSession
        }

        val transaction = firstState.shouldBeInstanceOf<TransactionStateOverlay<BigQueryEngine>>()
        val firstSession = transaction.parent.shouldBeInstanceOf<SessionState>()
        val secondSession = secondState.shouldBeInstanceOf<SessionState>()
        secondSession shouldNotBe firstSession
        verify(persistentState, never()).writeTable(any(), any())
    }

    context(connection : Connection<BigQueryEngine>)
    private fun captureTopState() : EngineState<BigQueryEngine> {
        val select = mock<Select<Any>>()
        whenever(context(any<EngineState<BigQueryEngine>>()) { queryEvaluator.selectRows(eq(select), eq(null)) }).thenReturn(emptyList())
        context(connection) { bigQueryEngineSimulator.execute(select, Any::class) }
        return argumentCaptor<EngineState<BigQueryEngine>>().run {
            verify(queryEvaluator).run { context(capture()) { selectRows(eq(select), eq(null)) } }
            clearInvocations(queryEvaluator)
            singleValue
        }
    }
}
