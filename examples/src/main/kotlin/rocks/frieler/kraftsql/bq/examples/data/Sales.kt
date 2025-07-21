package rocks.frieler.kraftsql.bq.examples.data

import rocks.frieler.kraftsql.bq.objects.Table
import java.time.Instant

data class Sale(
    val productId: Long,
    val storeId: Long,
    val time: Instant,
    val amount: Int,
) {
    constructor(product: Product, shop: Shop, time: Instant, amount: Int) : this(product.id, shop.id, time, amount)
}

val sales = Table(dataset = "examples", name = "sales", type = Sale::class)
