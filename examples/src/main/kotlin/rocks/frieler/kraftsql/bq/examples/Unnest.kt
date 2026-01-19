package rocks.frieler.kraftsql.bq.examples

import rocks.frieler.kraftsql.bq.dql.CorrelatedInnerJoin
import rocks.frieler.kraftsql.bq.dql.Select
import rocks.frieler.kraftsql.bq.examples.data.Product
import rocks.frieler.kraftsql.bq.examples.data.products
import rocks.frieler.kraftsql.bq.examples.data.withSampleData
import rocks.frieler.kraftsql.bq.expressions.Unnest
import rocks.frieler.kraftsql.bq.objects.collect
import rocks.frieler.kraftsql.dql.QuerySource
import rocks.frieler.kraftsql.dsl.`as`
import rocks.frieler.kraftsql.objects.DataRow

fun main() {
    withSampleData {
        Select<DataRow>(
            source = QuerySource(products),
            joins = listOf(
                CorrelatedInnerJoin(Unnest(products[Product::tags]) `as` "tag")
            )
        )
            .also { println(it.sql()) }
            .collect()
            .forEach { println(it) }
    }
}
