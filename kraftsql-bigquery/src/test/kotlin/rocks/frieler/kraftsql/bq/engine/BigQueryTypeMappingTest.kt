package rocks.frieler.kraftsql.bq.engine

import com.google.cloud.bigquery.StandardSQLTypeName
import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import rocks.frieler.kraftsql.data.Type

class BigQueryTypeMappingTest {
    @Test
    fun `Integer types are mapped to INT64`() {
        Type.INTEGER.native shouldBe StandardSQLTypeName.INT64
        Type.NULLABLE_INTEGER.native shouldBe StandardSQLTypeName.INT64
    }

    @Test
    fun `String types are mapped to STRING`() {
        Type.STRING.native shouldBe StandardSQLTypeName.STRING
        Type.NULLABLE_STRING.native shouldBe StandardSQLTypeName.STRING
    }

    @Test
    fun `an unknown Type cannot be mapped`() {
        shouldThrow<IllegalStateException> {
            object : Type { override fun toString(): String = "_unknown_" }.native
        }
    }
}
