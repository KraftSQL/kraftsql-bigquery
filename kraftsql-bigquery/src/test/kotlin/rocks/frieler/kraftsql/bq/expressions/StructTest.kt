package rocks.frieler.kraftsql.bq.expressions

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.expressions.Expression
import rocks.frieler.kraftsql.objects.DataRow

class StructTest {
    @Test
    fun `sql() for null-Struct returns NULL value`() {
        Struct<DataRow>(null).sql() shouldBe "NULL"
    }

    @Test
    fun `sql() builds typeless STRUCT with named fields`() {
        val expression1 = mock<Expression<BigQueryEngine, *>> { whenever(it.sql()).thenReturn("exp1") }
        val expression2 = mock<Expression<BigQueryEngine, *>> { whenever(it.sql()).thenReturn("exp2") }

        val sqlString = Struct<DataRow>(mapOf("e1" to expression1, "e2" to expression2)).sql()

        sqlString shouldBe "STRUCT(exp1 AS `e1`, exp2 AS `e2`)"
    }
}
