package rocks.frieler.kraftsql.bq.ddl

import rocks.frieler.kraftsql.bq.engine.BigQueryConnection
import rocks.frieler.kraftsql.bq.objects.Table
import rocks.frieler.kraftsql.ddl.create

fun <T : Any> Table<T>.create() {
    create(BigQueryConnection.Default.get())
}
