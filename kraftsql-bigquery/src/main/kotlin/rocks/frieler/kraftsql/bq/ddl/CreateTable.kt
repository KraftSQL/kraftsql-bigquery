package rocks.frieler.kraftsql.bq.ddl

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.objects.Table
import rocks.frieler.kraftsql.ddl.create

fun <T : Any> Table<T>.create() {
    create(BigQueryEngine.DefaultConnection.get())
}
