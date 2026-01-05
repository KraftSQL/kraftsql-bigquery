package rocks.frieler.kraftsql.bq.expressions

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.string.shouldMatch
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.expressions.Expression

class ArrayConcatTest {
    @Test
    fun `SQL calls ARRAY_CONCAT with arguments`() {
        val array1 = mock<Expression<BigQueryEngine, Array<Any>?>> { whenever(it.sql()).thenReturn("array1") }
        val array2 = mock<Expression<BigQueryEngine, Array<Any>?>> { whenever(it.sql()).thenReturn("array2") }

        val sql = ArrayConcat(array1, array2).sql()

        sql shouldMatch "^ARRAY_CONCAT\\(${array1.sql()},\\s*${array2.sql()}\\)$"
    }

    @Test
    fun `default column name cannot be known in BigQuery`() {
        shouldThrow<IllegalStateException> {
            ArrayConcat<Any>().defaultColumnName()
        }
    }
}
