package rocks.frieler.kraftsql.bq.testing

import org.junit.jupiter.api.extension.ExtendWith
import rocks.frieler.kraftsql.bq.engine.BigQueryConnection
import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.engine.DefaultConnection
import rocks.frieler.kraftsql.testing.SimulatorTestExtension

class BigQuerySimulatorTestExtension(
    override val connectionProvider : () -> BigQuerySimulatorConnection = { BigQuerySimulatorConnection() },
    defaultConnectionToConfigure: DefaultConnection<BigQueryEngine, BigQueryConnection>? = BigQueryEngine.DefaultConnection,
) : SimulatorTestExtension<BigQueryEngine, BigQueryConnection, BigQuerySimulatorConnection>(connectionProvider, defaultConnectionToConfigure) {

    class Builder(
        override val connectionProvider : () -> BigQuerySimulatorConnection = { BigQuerySimulatorConnection() },
    ) : SimulatorTestExtension.Builder<BigQueryEngine, BigQueryConnection, BigQuerySimulatorConnection>(connectionProvider) {

        init {
            defaultConnectionToConfigure(BigQueryEngine.DefaultConnection)
        }

        override fun build(): BigQuerySimulatorTestExtension {
            return BigQuerySimulatorTestExtension(this@Builder.connectionProvider, defaultConnectionToConfigure)
        }
    }
}

@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
@ExtendWith(BigQuerySimulatorTestExtension::class)
annotation class WithBigQuerySimulator
