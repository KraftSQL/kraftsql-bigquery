package rocks.frieler.kraftsql.bq.examples.data

import rocks.frieler.kraftsql.bq.objects.Table

data class Product(
    val id: Long,
    val name: String,
    val category: String,
)

val products = Table(dataset = "examples", name = "products", type = Product::class)
