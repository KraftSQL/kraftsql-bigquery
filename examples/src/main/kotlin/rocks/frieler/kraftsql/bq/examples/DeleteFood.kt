package rocks.frieler.kraftsql.bq.examples

import rocks.frieler.kraftsql.bq.examples.data.Category
import rocks.frieler.kraftsql.bq.examples.data.Product
import rocks.frieler.kraftsql.bq.examples.data.products
import rocks.frieler.kraftsql.expressions.`=`
import rocks.frieler.kraftsql.bq.ddl.create
import rocks.frieler.kraftsql.bq.ddl.drop
import rocks.frieler.kraftsql.bq.dml.delete
import rocks.frieler.kraftsql.bq.dml.insertInto
import rocks.frieler.kraftsql.bq.dql.execute
import rocks.frieler.kraftsql.bq.dsl.Select
import rocks.frieler.kraftsql.bq.expressions.Constant
import rocks.frieler.kraftsql.bq.objects.Table
import rocks.frieler.kraftsql.objects.DataRow
import kotlin.collections.forEach

fun main() {
    try {
        products.create()
        Product(1, "Apple", Category(1, "Food")).insertInto(products)
        Product(2, "Pants", Category(2, "Clothes")).insertInto(products)

        deleteFood(products).also { println("Deleted $it products.") }

        Select<DataRow> { from(products) }
            .execute()
            .forEach { println(it) }

    } finally {
        products.drop(ifExists = true)
    }
}

fun deleteFood(productTable: Table<Product>) =
    productTable.delete(productTable[Product::category][Category::name] `=` Constant("Food"))
