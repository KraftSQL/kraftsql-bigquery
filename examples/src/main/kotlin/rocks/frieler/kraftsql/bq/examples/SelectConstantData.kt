package rocks.frieler.kraftsql.bq.examples

import rocks.frieler.kraftsql.bq.objects.ConstantData
import rocks.frieler.kraftsql.bq.queries.Select
import rocks.frieler.kraftsql.bq.queries.execute
import rocks.frieler.kraftsql.objects.Row
import rocks.frieler.kraftsql.queries.QuerySource

fun main() {
    Select<Row>(
        source = QuerySource(ConstantData(Row(mapOf("foo" to "bar", "foo" to "baz")))),
    )
        .execute()
        .also { println(it.count()) }
}
