package rocks.frieler.kraftsql.bq.examples

import io.kotest.matchers.collections.shouldBeEmpty
import io.kotest.matchers.collections.shouldContainAll
import io.kotest.matchers.maps.shouldBeEmpty
import io.kotest.matchers.maps.shouldContain
import io.kotest.matchers.maps.shouldContainKeys
import org.junit.jupiter.api.Test
import rocks.frieler.kraftsql.dql.Projection
import rocks.frieler.kraftsql.bq.examples.data.Category
import rocks.frieler.kraftsql.bq.examples.data.Product
import rocks.frieler.kraftsql.dql.QuerySource
import rocks.frieler.kraftsql.bq.dql.Select
import rocks.frieler.kraftsql.bq.dql.execute
import rocks.frieler.kraftsql.expressions.Column
import rocks.frieler.kraftsql.bq.objects.ConstantData
import rocks.frieler.kraftsql.objects.DataRow
import rocks.frieler.kraftsql.bq.testing.WithBigQuerySimulator

@WithBigQuerySimulator
class ProductKeywordsTest {
    private val food = Category(1, "Food")

    @Test
    fun `collectProductKeywords() can handle empty data`() {
        val products = ConstantData<Product>()

        val keywords = Select<DataRow>(
            QuerySource(collectProductKeywords(products)),
            columns = listOf(Projection(Column("keywords"))),
        ).execute()

        keywords.shouldBeEmpty()
    }

    @Test
    fun `collectProductKeywords() collects name, category and tags per Product`() {
        val products = ConstantData(
            Product(1, "Chocolate", food, tags = arrayOf("sweets")),
            Product(2, "Lemon", food, tags = arrayOf("sour")),
        )

        val keywords = Select<DataRow>(
            QuerySource(collectProductKeywords(products)),
            columns = listOf(Projection(Column("keywords"))),
        ).execute()

        keywords.shouldContainAll(
            DataRow(mapOf("keywords" to arrayOf("Chocolate", "Food", "sweets"))),
            DataRow(mapOf("keywords" to arrayOf("Lemon", "Food", "sour"))),
        )
    }

    @Test
    fun `countKeywords() can handle empty data`() {
        val words = ConstantData(emptyList<DataRow>())

        val wordCounts = countKeywords(words)

        wordCounts.shouldBeEmpty()
    }

    @Test
    fun `countKeywords() collects all words`() {
        val words = ConstantData(
            DataRow(mapOf("keywords" to arrayOf("sweets"))),
            DataRow(mapOf("keywords" to arrayOf("sour"))),
        )

        val wordCounts = countKeywords(words)

        wordCounts.shouldContainKeys("sweets", "sour")
    }

    @Test
    fun `countKeywords() counts occurrences per word`() {
        val words = ConstantData<DataRow>(
            DataRow(mapOf("keywords" to arrayOf("fruit", "sweet"))),
            DataRow(mapOf("keywords" to arrayOf("fruit", "sour"))),
        )

        val wordCounts = countKeywords(words)

        wordCounts.shouldContain("fruit", 2L)
        wordCounts.shouldContain("sweet", 1L)
        wordCounts.shouldContain("sour", 1L)
    }
}
