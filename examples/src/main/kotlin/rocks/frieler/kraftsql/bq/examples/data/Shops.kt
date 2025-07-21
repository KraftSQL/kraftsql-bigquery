package rocks.frieler.kraftsql.bq.examples.data

import rocks.frieler.kraftsql.bq.objects.Table

data class Shop(
    val id: Long,
    val country: String,
)

val shops = Table(dataset = "examples", name = "shops", type = Shop::class)
