package rocks.frieler.kraftsql.bq.examples

import rocks.frieler.kraftsql.dsl.`as`
import rocks.frieler.kraftsql.bq.examples.data.Category
import rocks.frieler.kraftsql.bq.examples.data.Product
import rocks.frieler.kraftsql.bq.examples.data.products
import rocks.frieler.kraftsql.bq.dsl.Select
import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.examples.data.withSampleData
import rocks.frieler.kraftsql.bq.expressions.ArrayConcat
import rocks.frieler.kraftsql.bq.expressions.Unnest
import rocks.frieler.kraftsql.bq.objects.collect
import rocks.frieler.kraftsql.expressions.Count
import rocks.frieler.kraftsql.objects.Data
import rocks.frieler.kraftsql.objects.DataRow

fun main() {
    withSampleData {
        val keywordCounts = collectProductKeywords(products)
        keywordCounts.collect()
            .forEach { println(it) }
    }
}

fun collectProductKeywords(products: Data<BigQueryEngine, Product>) =
    Select<DataRow> {
        from(products)
        val keyword = crossJoin(Unnest(ArrayConcat(
            rocks.frieler.kraftsql.expressions.Array(products[Product::name], products[Product::category][Category::name]),
            products[Product::tags],
        )) `as` "keyword")
        groupBy(keyword["keyword"])
        columns(
            keyword["keyword"] `as` "keyword",
            Count<BigQueryEngine>() `as` "count",
        )
    }
