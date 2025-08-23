package rocks.frieler.kraftsql.bq.examples

import rocks.frieler.kraftsql.dql.Projection
import rocks.frieler.kraftsql.dsl.`as`
import rocks.frieler.kraftsql.bq.examples.data.Category
import rocks.frieler.kraftsql.bq.examples.data.Product
import rocks.frieler.kraftsql.bq.examples.data.products
import rocks.frieler.kraftsql.bq.ddl.create
import rocks.frieler.kraftsql.bq.ddl.drop
import rocks.frieler.kraftsql.bq.dml.insertInto
import rocks.frieler.kraftsql.bq.dql.execute
import rocks.frieler.kraftsql.bq.dsl.Select
import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.objects.ConstantData
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
            rocks.frieler.kraftsql.expressions.Array(
                products[Product::name],
                products[Product::category][Category::name]
            ) `as` "part1",
            products[Product::tags] `as` "part2",
        )
    }
        .execute()
        .map {
            // TODO: Concat arrays in SQL, once this is implemented.
            @Suppress("UNCHECKED_CAST")
            DataRow(mapOf("keywords" to (it["part1"] as Array<String> + it["part2"] as Array<String>)))
        }
        .let { ConstantData(it) }

fun countKeywords(words: Data<BigQueryEngine, DataRow>): Map<String, Long> {
    val wordCounts = Select<DataRow> {
        from(words)
        columns(Projection(words["keywords"]))
    }.execute()
        .map { row ->
            @Suppress("UNCHECKED_CAST")
            row["keywords"] as Array<String>
        }
        .fold(mutableMapOf<String, Long>()) { stats, tags ->
            tags.forEach { tag ->
                stats.compute(tag) { _, count -> count?.plus(1) ?: 1 }
            }
            stats
        }
    return wordCounts
}
