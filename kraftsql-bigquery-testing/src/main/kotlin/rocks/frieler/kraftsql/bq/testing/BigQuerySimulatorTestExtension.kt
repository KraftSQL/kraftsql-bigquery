package rocks.frieler.kraftsql.bq.testing

import org.junit.jupiter.api.extension.ExtendWith
import rocks.frieler.kraftsql.bq.engine.BigQueryConnection
import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.engine.DefaultConnection
import rocks.frieler.kraftsql.testing.SimulatorTestExtension
import rocks.frieler.kraftsql.testing.engine.SimulatorConnection

class BigQuerySimulatorTestExtension(
    connection : SimulatorConnection<BigQueryEngine> = SimulatorConnection(),
    defaultConnectionToConfigure: DefaultConnection<BigQueryEngine>? = BigQueryConnection.Default,
) : SimulatorTestExtension<BigQueryEngine>(connection, defaultConnectionToConfigure) {

    class Builder(
        connection : SimulatorConnection<BigQueryEngine> = SimulatorConnection(),
    ) : SimulatorTestExtension.Builder<BigQueryEngine>(connection) {

        init {
            defaultConnectionToConfigure(BigQueryConnection.Default)
        }

        override fun build(): BigQuerySimulatorTestExtension {
            return BigQuerySimulatorTestExtension(this@Builder.connection, defaultConnectionToConfigure)
        }
    }
}

@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
@ExtendWith(BigQuerySimulatorTestExtension::class)
annotation class WithBigQuerySimulator
