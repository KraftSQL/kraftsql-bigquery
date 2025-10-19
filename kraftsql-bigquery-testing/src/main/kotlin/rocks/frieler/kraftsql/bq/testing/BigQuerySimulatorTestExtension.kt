package rocks.frieler.kraftsql.bq.testing

import org.junit.jupiter.api.extension.ExtendWith
import org.junit.jupiter.api.extension.ExtensionContext
import rocks.frieler.kraftsql.bq.engine.BigQueryConnection
import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.testing.engine.BigQuerySimulatorConnection
import rocks.frieler.kraftsql.engine.DefaultConnection
import rocks.frieler.kraftsql.testing.SimulatorTestExtension
import kotlin.jvm.optionals.getOrNull

class BigQuerySimulatorTestExtension(
    override val connectionProvider : (ExtensionContext) -> BigQuerySimulatorConnection = { extensionContext ->
        BigQuerySimulatorConnection().apply {
            val extensionAnnotation = extensionContext.testClass.getOrNull()?.getAnnotation(WithBigQuerySimulator::class.java)
            if (extensionAnnotation != null) {
                setSessionMode(extensionAnnotation.sessionMode)
            }
        }},
    defaultConnectionToConfigure: DefaultConnection<BigQueryEngine, BigQueryConnection>? = BigQueryEngine.DefaultConnection,
) : SimulatorTestExtension<BigQueryEngine, BigQueryConnection, BigQuerySimulatorConnection>(connectionProvider, defaultConnectionToConfigure) {

    class Builder(
        override val connectionProvider : () -> BigQuerySimulatorConnection = { BigQuerySimulatorConnection() },
    ) : SimulatorTestExtension.Builder<BigQueryEngine, BigQueryConnection, BigQuerySimulatorConnection>(connectionProvider) {

        init {
            defaultConnectionToConfigure(BigQueryEngine.DefaultConnection)
        }

        override fun build(): BigQuerySimulatorTestExtension {
            return BigQuerySimulatorTestExtension({ this@Builder.connectionProvider.invoke() }, defaultConnectionToConfigure)
        }
    }
}

@Target(AnnotationTarget.CLASS)
@Retention(AnnotationRetention.RUNTIME)
@ExtendWith(BigQuerySimulatorTestExtension::class)
annotation class WithBigQuerySimulator(
    val sessionMode: Boolean = false,
)
