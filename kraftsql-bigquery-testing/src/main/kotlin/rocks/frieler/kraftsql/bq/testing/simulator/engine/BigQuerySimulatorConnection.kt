package rocks.frieler.kraftsql.bq.testing.simulator.engine

import rocks.frieler.kraftsql.bq.dml.LoadData
import rocks.frieler.kraftsql.bq.engine.BigQueryConnection
import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.testing.simulator.engine.GenericSimulatorConnection

/**
 * [rocks.frieler.kraftsql.testing.simulator.engine.SimulatorConnection] for the [BigQueryEngine].
 *
 * @param engine the [BigQueryEngineSimulator] to connect to
 */
class BigQuerySimulatorConnection(
    override val engine: BigQueryEngineSimulator = BigQueryEngineSimulator(),
) : BigQueryConnection, GenericSimulatorConnection<BigQueryEngine>(engine) {
    override fun execute(loadData: LoadData) {
        engine.execute(loadData)
    }

    override var sessionMode = false
        set(newSessionMode) {
            if (this.sessionMode && !newSessionMode) {
                engine.abortSession()
            }
            field = newSessionMode
        }
}
