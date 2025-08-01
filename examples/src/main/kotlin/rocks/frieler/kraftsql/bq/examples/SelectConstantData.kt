package rocks.frieler.kraftsql.bq.examples

import rocks.frieler.kraftsql.bq.objects.ConstantData
import rocks.frieler.kraftsql.bq.dql.Select
import rocks.frieler.kraftsql.bq.dql.execute
import rocks.frieler.kraftsql.objects.Row
import rocks.frieler.kraftsql.dql.QuerySource

fun main() {
    Select<Row>(
        source = QuerySource(ConstantData(Row(mapOf("foo" to "bar", "foo" to "baz")))),
    )
        .execute()
        .also { println(it.count()) }
}
