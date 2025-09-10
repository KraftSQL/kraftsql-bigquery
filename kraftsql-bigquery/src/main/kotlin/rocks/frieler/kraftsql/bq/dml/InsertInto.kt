package rocks.frieler.kraftsql.bq.dml

import rocks.frieler.kraftsql.dml.insertInto
import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.engine.BigQueryORMapping
import rocks.frieler.kraftsql.objects.Data
import rocks.frieler.kraftsql.objects.Table

fun <T : Any> Data<BigQueryEngine, T>.insertInto(table: Table<BigQueryEngine, T>) =
    insertInto(table, BigQueryEngine.DefaultConnection.get())

fun <T : Any> T.insertInto(table: Table<BigQueryEngine, T>) =
    this.insertInto(BigQueryORMapping, table, BigQueryEngine.DefaultConnection.get())
