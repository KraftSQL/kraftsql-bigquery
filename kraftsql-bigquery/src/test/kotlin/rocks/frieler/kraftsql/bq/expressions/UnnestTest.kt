package rocks.frieler.kraftsql.bq.expressions

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.expressions.Expression

class UnnestTest {
    private val arrayExpression = mock<Expression<BigQueryEngine, Array<Any>>>()
    private val unnest = Unnest(arrayExpression)

    @Test
    fun `default column name is UNNEST({default column name of Array Expression})`() {
        whenever(arrayExpression.defaultColumnName()).thenReturn("array")

        unnest.defaultColumnName() shouldBe "UNNEST(array)"
    }

    @Test
    fun `UNEST has only a single unnamed sub-column`() {
        unnest.columnNames shouldBe listOf("")
    }

    @Test
    fun `sql() returns UNNEST({SQL of Array Expression})`() {
        whenever(arrayExpression.sql()).thenReturn("array")

        unnest.sql() shouldBe "UNNEST(array)"
    }

    @Test
    fun `Unnests of equal Array Expression are equal`() {
        (unnest == Unnest(arrayExpression)) shouldBe true
    }

    @Test
    fun `Unnests of different Array Expressions are not equal`() {
        (unnest == Unnest(mock<Expression<BigQueryEngine, Array<Any>>>())) shouldBe false
    }

    @Test
    fun `Unnest and something else are not equal`() {
        (unnest == Any()) shouldBe false
    }

    @Test
    fun `Equal Unnests have same hash code`() {
        Unnest(arrayExpression).hashCode() shouldBe unnest.hashCode()
    }
}
