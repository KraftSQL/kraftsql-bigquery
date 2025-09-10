package rocks.frieler.kraftsql.bq.dsl

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.dsl.inTransaction

fun inTransaction(content: () -> Unit) {
    inTransaction(BigQueryEngine.DefaultConnection.get(), content)
}
