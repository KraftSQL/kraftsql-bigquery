package rocks.frieler.kraftsql.bq.engine

import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.string.shouldMatch
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import rocks.frieler.kraftsql.bq.engine.Types.STRUCT
import rocks.frieler.kraftsql.objects.Column

class TypesTest {
    @Test
    fun `STRUCT renders SQL with subfields in angle brackets`() {
        val structType = STRUCT(listOf(
            mock { whenever(it.name).thenReturn("foo"); whenever(it.type).thenReturn(Types.STRING) },
            mock { whenever(it.name).thenReturn("bar"); whenever(it.type).thenReturn(Types.INT64) },
        ))

        structType.sql() shouldMatch "STRUCT<foo\\s+STRING,\\s*bar\\s+INT64>"
    }

    @Test
    fun `STRUCT parses subfields from SQL string`() {
        val structType = STRUCT.parse("STRUCT<foo STRING, bar INT64>")

        structType.fields shouldContainExactly listOf(
            Column("foo", Types.STRING, nullable = true),
            Column("bar", Types.INT64, nullable = true),
        )
    }
}
