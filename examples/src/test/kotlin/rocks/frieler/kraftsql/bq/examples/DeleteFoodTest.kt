package rocks.frieler.kraftsql.bq.examples

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import rocks.frieler.kraftsql.dsl.`as`
import rocks.frieler.kraftsql.bq.dsl.Select
import rocks.frieler.kraftsql.bq.examples.data.Category
import rocks.frieler.kraftsql.bq.examples.data.Product
import rocks.frieler.kraftsql.bq.examples.data.products
import rocks.frieler.kraftsql.expressions.Count
import rocks.frieler.kraftsql.bq.ddl.create
import rocks.frieler.kraftsql.bq.dml.insertInto
import rocks.frieler.kraftsql.bq.dql.execute
import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.testing.WithBigQuerySimulator
import rocks.frieler.kraftsql.objects.DataRow
import rocks.frieler.kraftsql.testing.matchers.collections.shouldContainExactlyOne
import rocks.frieler.kraftsql.testing.matchers.collections.shouldContainNone

@WithBigQuerySimulator
class DeleteFoodTest {
    @Test
    fun `deleteFood() deletes products from category Food`() {
        products.create()
        Product(1, "Apple", Category(1, "Food")).insertInto(products)
        Product(2, "Pants", Category(2, "Clothes")).insertInto(products)

        val deletedProductCount = deleteFood(products)

        deletedProductCount shouldBe 1
        val remainingProductsPerCategory = Select<DataRow> {
            from(products)
            columns(
                products[Product::category][Category::name] `as` "category",
                Count<BigQueryEngine>() `as` "products",
            )
            groupBy(products[Product::category][Category::name])
        }.execute()
        val clothes = remainingProductsPerCategory shouldContainExactlyOne { it["category"] == "Clothes" }
        clothes["products"] shouldBe 1L
        remainingProductsPerCategory shouldContainNone { it["category"] == "Food" }
    }
}
