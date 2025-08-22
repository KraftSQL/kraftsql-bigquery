package rocks.frieler.kraftsql.bq.examples.data

import rocks.frieler.kraftsql.bq.objects.Table

data class Product(
    val id: Long,
    val name: String,
    val category: Category,
    val tags: Array<String> = arrayOf(),
) {

    override fun equals(other: Any?) = other is Product
            && id == other.id
            && name == other.name
            && category == other.category
            && tags.contentEquals(other.tags)

    override fun hashCode() = id.hashCode()
}

val products = Table(dataset = "examples", name = "products", type = Product::class)
