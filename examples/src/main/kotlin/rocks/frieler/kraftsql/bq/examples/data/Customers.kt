package rocks.frieler.kraftsql.bq.examples.data

import rocks.frieler.kraftsql.bq.objects.Table
import java.time.LocalDate

data class Customer(
    val id: Long,
    val country: Country,
    val dateOfBirth: LocalDate,
)

val customers = Table(dataset = "examples", name = "customers", type = Customer::class)
