package rocks.frieler.kraftsql.bq.examples

import rocks.frieler.kraftsql.dsl.`as`
import rocks.frieler.kraftsql.bq.examples.data.Customer
import rocks.frieler.kraftsql.bq.examples.data.Purchase
import rocks.frieler.kraftsql.bq.examples.data.customers
import rocks.frieler.kraftsql.bq.examples.data.purchases
import rocks.frieler.kraftsql.bq.examples.data.withSampleData
import rocks.frieler.kraftsql.expressions.`=`
import rocks.frieler.kraftsql.expressions.Sum
import rocks.frieler.kraftsql.bq.dsl.Select
import rocks.frieler.kraftsql.bq.expressions.Constant
import rocks.frieler.kraftsql.bq.objects.Data
import rocks.frieler.kraftsql.bq.objects.collect
import rocks.frieler.kraftsql.expressions.Coalesce
import rocks.frieler.kraftsql.expressions.Count
import java.math.BigDecimal

fun main() {
    withSampleData {
        aggregatePurchaseValuePerCustomer(customers, purchases)
            .collect()
            .forEach { println(it) }
    }
}

data class CustomerPurchaseValue(val customerId: Long, val purchases: Long, val totalAmount: BigDecimal)

fun aggregatePurchaseValuePerCustomer(customers: Data<Customer>, purchases: Data<Purchase>) : Data<CustomerPurchaseValue> =
    Select {
        val c = from(customers `as` "c")
        val p = leftJoin(purchases `as` "p") { this[Purchase::customerId] `=` c[Customer::id] }
        columns(
            c[Customer::id] `as` CustomerPurchaseValue::customerId,
            Count(p[Purchase::id]) `as` CustomerPurchaseValue::purchases,
            Coalesce(
                Sum(p[Purchase::totalPrice]),
                nonNullableExpression = Constant(BigDecimal.ZERO),
            ) `as` CustomerPurchaseValue::totalAmount,
        )
        groupBy(c[Customer::id])
    }
