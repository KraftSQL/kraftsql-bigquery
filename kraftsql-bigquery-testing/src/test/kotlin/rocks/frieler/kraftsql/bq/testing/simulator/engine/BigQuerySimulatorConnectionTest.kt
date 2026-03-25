package rocks.frieler.kraftsql.bq.testing.simulator.engine

import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.verify
import rocks.frieler.kraftsql.bq.dml.LoadData

class BigQuerySimulatorConnectionTest {
    private val engine = mock<BigQueryEngineSimulator>()
    private val connection = BigQuerySimulatorConnection(engine)

    @Test
    fun `execute LoadData delegates to engine`() {
        val loadData = mock<LoadData>()

        connection.execute(loadData)

        verify(engine).run { context(connection) { execute(loadData) }}
    }

    @Test
    fun `deactivating session mode aborts the session`() {
        connection.sessionMode = true
        connection.sessionMode = false

        verify(engine).run { context(connection) { abortSession() }}
    }
}
