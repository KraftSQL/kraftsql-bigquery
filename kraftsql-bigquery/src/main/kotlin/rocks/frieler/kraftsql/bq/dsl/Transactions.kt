package rocks.frieler.kraftsql.bq.dsl

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.dsl.transaction

fun transaction(content: () -> Unit) {
    transaction(BigQueryEngine.DefaultConnection.get(), content)
}
