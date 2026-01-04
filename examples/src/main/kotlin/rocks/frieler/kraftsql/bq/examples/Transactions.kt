package rocks.frieler.kraftsql.bq.examples

import rocks.frieler.kraftsql.expressions.`=`
import rocks.frieler.kraftsql.bq.dml.Delete
import rocks.frieler.kraftsql.bq.dml.execute
import rocks.frieler.kraftsql.bq.dml.insertInto
import rocks.frieler.kraftsql.bq.dql.execute
import rocks.frieler.kraftsql.bq.dsl.Select
import rocks.frieler.kraftsql.bq.dsl.transaction
import rocks.frieler.kraftsql.bq.engine.Types.INT64
import rocks.frieler.kraftsql.bq.examples.data.with
import rocks.frieler.kraftsql.bq.expressions.Constant
import rocks.frieler.kraftsql.bq.objects.Table
import rocks.frieler.kraftsql.objects.Column
import rocks.frieler.kraftsql.objects.DataRow
import kotlin.collections.forEach
import kotlin.to

fun main() {
    val table = Table<DataRow>(dataset = "examples", name = "something", columns = listOf(Column("number", INT64)))
    with(table) {
        DataRow("number" to 1).insertInto(table)
        DataRow("number" to 2).insertInto(table)

        try {
            transaction {
                Delete(table, table["number"] `=` Constant(1)).execute()
                DataRow("number" to 3, "foo" to "bar").insertInto(table)
            }
        } catch (e: Exception) {
            println(e.message)
        } finally {
            Select<DataRow> { from(table) }.execute()
                .forEach { println(it) }
        }
    }
}
