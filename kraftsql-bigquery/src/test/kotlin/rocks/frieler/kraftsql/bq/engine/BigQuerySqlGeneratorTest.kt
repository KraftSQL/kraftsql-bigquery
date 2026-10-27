package rocks.frieler.kraftsql.bq.engine

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import rocks.frieler.kraftsql.data.ConstantData
import rocks.frieler.kraftsql.data.DataRow
import rocks.frieler.kraftsql.data.Selection
import rocks.frieler.kraftsql.expressions.get

class BigQuerySqlGeneratorTest {
    @Test
    fun `makeSQL selects ConstantData of DataRows from an array of structs in their original order`() {
        val data = ConstantData(
            DataRow("number" to 1, "name" to "one"),
            DataRow("number" to 2, "name" to "two"),
        )

        BigQuerySqlGenerator.makeSQL(data).normalized() shouldBe
            "SELECT `number`,`name` " +
            "FROM UNNEST(ARRAY<STRUCT<`number` INT64,`name` STRING>>[STRUCT(1,'one'),STRUCT(2,'two')]) " +
            "WITH OFFSET AS row_number " +
            "ORDER BY row_number"
    }

    @Test
    fun `makeSQL orders by an offset column whose name does not collide with a row_number column`() {
        val data = ConstantData(
            DataRow("row_number" to 1, "name" to "one"),
            DataRow("row_number" to 2, "name" to "two"),
        )

        BigQuerySqlGenerator.makeSQL(data).normalized() shouldBe
            "SELECT `row_number`,`name` " +
            "FROM UNNEST(ARRAY<STRUCT<`row_number` INT64,`name` STRING>>[STRUCT(1,'one'),STRUCT(2,'two')]) " +
            "WITH OFFSET AS _row_number " +
            "ORDER BY _row_number"
    }

    @Test
    fun `constant() renders null as NULL`() {
        BigQuerySqlGenerator.constant(null) shouldBe "NULL"
    }

    @Test
    fun `constant() renders an Int as integer literal`() {
        BigQuerySqlGenerator.constant(42) shouldBe "42"
    }

    @Test
    fun `constant() renders a String as single-quoted string literal`() {
        BigQuerySqlGenerator.constant("text") shouldBe "'text'"
    }

    @Test
    fun `constant() escapes special characters in a String`() {
        val stringWithSpecialCaharcters = """
            it's a \ test
            with a second line
        """.trimIndent()

        BigQuerySqlGenerator.constant(stringWithSpecialCaharcters.trimMargin()) shouldBe """'it\'s a \\ test\nwith a second line'"""
    }

    @Test
    fun `makeSQL selects the columns of a Selection from its source data as subquery`() {
        val data = ConstantData(
            DataRow("number" to 1, "name" to "one"),
            DataRow("number" to 2, "name" to "two"),
        )
        val selection = Selection(data, listOf(data.get<String>("name")))

        BigQuerySqlGenerator.makeSQL(selection).normalized() shouldBe
            "SELECT `name` " +
            "FROM (" +
                "SELECT `number`,`name` " +
                "FROM UNNEST(ARRAY<STRUCT<`number` INT64,`name` STRING>>[STRUCT(1,'one'),STRUCT(2,'two')]) " +
                "WITH OFFSET AS row_number " +
                "ORDER BY row_number" +
            ")"
    }

    // ATTENTION: Does not respect String literal, but changes them as well!
    private fun String.normalized() = trim()
        .replace(Regex("""([(,\[])\s*"""), "$1")
        .replace(Regex("""\s*([),\]])"""), "$1")
        .replace(Regex("""\s+"""), " ")
}
