package rocks.frieler.kraftsql.bq.examples.data

import rocks.frieler.kraftsql.bq.objects.Table

data class Customer(
    val id: Long,
    val country: Country,
)

val customers = Table(dataset = "examples", name = "customers", type = Customer::class)
