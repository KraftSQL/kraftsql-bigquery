package rocks.frieler.kraftsql.bq.examples

import rocks.frieler.kraftsql.dsl.`as`
import rocks.frieler.kraftsql.bq.examples.data.Product
import rocks.frieler.kraftsql.bq.examples.data.PurchaseItem
import rocks.frieler.kraftsql.bq.examples.data.Customer
import rocks.frieler.kraftsql.bq.examples.data.products
import rocks.frieler.kraftsql.bq.examples.data.purchases
import rocks.frieler.kraftsql.bq.examples.data.customers
import rocks.frieler.kraftsql.expressions.`=`
import rocks.frieler.kraftsql.expressions.Sum
import rocks.frieler.kraftsql.bq.dsl.Select
import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.dql.execute
import rocks.frieler.kraftsql.objects.Data
import rocks.frieler.kraftsql.objects.DataRow
import rocks.frieler.kraftsql.bq.examples.data.Category
import rocks.frieler.kraftsql.bq.examples.data.Country
import rocks.frieler.kraftsql.bq.examples.data.ProductOutline
import rocks.frieler.kraftsql.bq.examples.data.Purchase
import rocks.frieler.kraftsql.bq.examples.data.withSampleData
import rocks.frieler.kraftsql.bq.expressions.Constant
import rocks.frieler.kraftsql.bq.expressions.Unnest

fun main() {
    withSampleData {
        calculateSoldFoodPerCountry(products, customers, purchases)
            .execute().forEach {
                println("${it[Customer::country.name]}: ${it["_totalAmount"]}")
            }
    }
}

fun calculateSoldFoodPerCountry(
    products: Data<BigQueryEngine, Product>,
    customers: Data<BigQueryEngine, Customer>,
    purchases: Data<BigQueryEngine, Purchase>,
) = Select<DataRow> {
    from(purchases)
    val items = crossJoin(Unnest(purchases[Purchase::items]) `as` "item")
    val products = innerJoin(products `as` "products") { this[Product::id] `=` items[PurchaseItem::product][ProductOutline::id] }
    val customers = innerJoin(customers `as` "customers") { this[Customer::id] `=` purchases[Purchase::customerId] }
    columns(
        customers[Customer::country][Country::code] `as` Customer::country.name,
        Sum.Companion(items[PurchaseItem::amount]) `as` "_totalAmount",
    )
    where(products[Product::category][Category::name] `=` Constant("Food"))
    groupBy(customers[Customer::country][Country::code])
}
