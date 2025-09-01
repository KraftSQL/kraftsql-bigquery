package rocks.frieler.kraftsql.bq.dsl

import rocks.frieler.kraftsql.bq.engine.BigQueryConnection
import rocks.frieler.kraftsql.dsl.inTransaction

fun inTransaction(content: () -> Unit) {
    inTransaction(BigQueryConnection.Default.get(), content)
}
