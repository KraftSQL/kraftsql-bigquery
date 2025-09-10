package rocks.frieler.kraftsql.bq.ddl

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.objects.Table
import rocks.frieler.kraftsql.ddl.drop

fun <T : Any> Table<T>.drop(ifExists: Boolean = false) {
    drop(BigQueryEngine.DefaultConnection.get(), ifExists)
}
