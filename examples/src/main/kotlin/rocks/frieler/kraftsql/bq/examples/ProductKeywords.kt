package rocks.frieler.kraftsql.bq.examples

import rocks.frieler.kraftsql.dsl.`as`
import rocks.frieler.kraftsql.bq.examples.data.Category
import rocks.frieler.kraftsql.bq.examples.data.Product
import rocks.frieler.kraftsql.bq.examples.data.products
import rocks.frieler.kraftsql.bq.ddl.create
import rocks.frieler.kraftsql.bq.ddl.drop
import rocks.frieler.kraftsql.bq.dml.insertInto
import rocks.frieler.kraftsql.bq.dsl.Select
import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.expressions.ArrayConcat
import rocks.frieler.kraftsql.bq.expressions.Unnest
import rocks.frieler.kraftsql.bq.objects.collect
import rocks.frieler.kraftsql.expressions.Count
import rocks.frieler.kraftsql.expressions.Expression
import rocks.frieler.kraftsql.objects.Data
import rocks.frieler.kraftsql.objects.DataRow

fun main() {
    try {
        val food = Category(1, "Food")
        val clothes = Category(2, "Clothes")
        val other = Category(3, "Other")

        products.create()
        Product(1, "Chocolate", food, tags = arrayOf("sweets", "snacks")).also { it.insertInto(products) }
        Product(2, "Pants", clothes, tags = arrayOf()).also { it.insertInto(products) }
        Product(3, "Crisps", food, tags = arrayOf("snacks")).also { it.insertInto(products) }
        Product(4, "Crap", other, tags = arrayOf("bullshit")).also { it.insertInto(products) }

        val productKeywords = collectProductKeywords(products)
        val wordCounts = countKeywords(productKeywords)
        wordCounts
            .forEach { (keywords, count) -> println("$keywords: $count") }

    } finally {
        products.drop(ifExists = true)
    }
}

fun collectProductKeywords(products: Data<BigQueryEngine, Product>) =
    Select<DataRow> {
        from(products)
        columns(
            ArrayConcat(
                rocks.frieler.kraftsql.expressions.Array(products[Product::name], products[Product::category][Category::name]),
                products[Product::tags],
            ) `as` "keywords",
        )
    }

fun countKeywords(words: Data<BigQueryEngine, DataRow>): Map<String, Long> {
    return Select<DataRow> {
        from(words)
        val keyword = crossJoin(Unnest(words["keywords"] as Expression<BigQueryEngine, Array<String>>) `as` "keyword")
        groupBy(keyword["keyword"])
        columns(
            keyword["keyword"] `as` "keyword",
            Count<BigQueryEngine>() `as` "count",
        )
    }
        .collect()
        .associate { it["keyword"] as String to it["count"] as Long }
}
