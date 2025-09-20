package rocks.frieler.kraftsql.bq.examples

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import rocks.frieler.kraftsql.bq.examples.data.Country
import rocks.frieler.kraftsql.bq.examples.data.Customer
import rocks.frieler.kraftsql.bq.examples.data.Purchase
import rocks.frieler.kraftsql.bq.testing.WithBigQuerySimulator
import rocks.frieler.kraftsql.bq.dql.execute
import rocks.frieler.kraftsql.bq.objects.ConstantData
import rocks.frieler.kraftsql.testing.kotest.inspectors.filterForOne
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDate

@WithBigQuerySimulator
class TotalPurchaseValuePerCustomerTest {
    private val country = Country("XX", "XXX")

    @Test
    fun `aggregates total purchase value per Customer`() {
        val customer1 = Customer(1, country, LocalDate.EPOCH)
        val customer2 = Customer(2, country, LocalDate.EPOCH)
        val customers = ConstantData(customer1, customer2)

        val purchases = ConstantData(
            Purchase(1, customer1, Instant.EPOCH, BigDecimal("1.00")),
            Purchase(2, customer1, Instant.EPOCH, BigDecimal("2.00")),
            Purchase(3, customer2, Instant.EPOCH, BigDecimal("10.00")),
            Purchase(4, customer2, Instant.EPOCH, BigDecimal("20.00")),
        )

        val customerPurchaseValues = aggregatePurchaseValuePerCustomer(customers, purchases)
            .execute()

        (customerPurchaseValues.filterForOne { it.customerId shouldBe customer1.id }).also {
            it.totalAmount shouldBe BigDecimal("3.00")
        }
        (customerPurchaseValues.filterForOne { it.customerId shouldBe customer2.id }).also {
            it.totalAmount shouldBe BigDecimal("30.00")
        }
    }
}
