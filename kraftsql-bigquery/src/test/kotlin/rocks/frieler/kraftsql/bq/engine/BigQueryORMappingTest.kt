package rocks.frieler.kraftsql.bq.engine

import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.should
import io.kotest.matchers.types.beOfType
import org.junit.jupiter.api.Test
import rocks.frieler.kraftsql.objects.Column
import kotlin.reflect.typeOf

class BigQueryORMappingTest {
    @Test
    fun `getTypeFor data class returns STRUCT Type with properties as fields`() {
        data class Something(val name: String, val value: Int?)

        val bqType = BigQueryORMapping.getTypeFor(typeOf<Something>())

        bqType should beOfType(Types.STRUCT::class)
        (bqType as Types.STRUCT).fields shouldContainExactly listOf(
            Column("name", Types.STRING, nullable = false),
            Column("value", Types.INT64, nullable = true),
        )
    }
}
