package rocks.frieler.kraftsql.bq.dml

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.objects.Table
import rocks.frieler.kraftsql.dml.execute
import rocks.frieler.kraftsql.expressions.Expression

class Delete(table: Table<*>, condition: Expression<BigQueryEngine, Boolean>) : rocks.frieler.kraftsql.dml.Delete<BigQueryEngine>(table, condition)

fun Delete.execute() = execute(BigQueryEngine.DefaultConnection.get())

fun Table<*>.delete(condition: Expression<BigQueryEngine, Boolean>) =
    Delete(this, condition).execute()
