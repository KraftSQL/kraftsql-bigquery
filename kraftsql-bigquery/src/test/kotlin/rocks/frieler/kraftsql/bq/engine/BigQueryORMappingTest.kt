package rocks.frieler.kraftsql.bq.engine

import com.google.cloud.bigquery.Field
import com.google.cloud.bigquery.FieldList
import com.google.cloud.bigquery.FieldValue
import com.google.cloud.bigquery.FieldValueList
import com.google.cloud.bigquery.LegacySQLTypeName
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.should
import io.kotest.matchers.shouldBe
import io.kotest.matchers.types.beOfType
import org.junit.jupiter.api.Test
import rocks.frieler.kraftsql.objects.Column
import rocks.frieler.kraftsql.objects.DataRow
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

    @Test
    fun `deserializeQueryResult can deserialize BOOL value`() {
        val result = BigQueryORMapping.deserializeQueryResult(listOf(
            mapOf(Field.of("value", Types.BOOL.name) to FieldValue.of(FieldValue.Attribute.PRIMITIVE, true.toString()))
        ), DataRow::class)

        result.single()["value"] shouldBe true
    }

    @Test
    fun `deserializeQueryResult can deserialize NULL as BOOL value`() {
        val result = BigQueryORMapping.deserializeQueryResult(listOf(
            mapOf(Field.of("value", Types.BOOL.name) to FieldValue.of(FieldValue.Attribute.PRIMITIVE, null))
        ), DataRow::class)

        result.single()["value"] shouldBe null
    }

    @Test
    fun `deserializeQueryResult can deserialize data class`() {
        data class DataClass(val value: String)

        val result = BigQueryORMapping.deserializeQueryResult(listOf(
            mapOf(Field.of("value", Types.STRING.name) to FieldValue.of(FieldValue.Attribute.PRIMITIVE, "test"))
        ), DataClass::class)

        result.single() shouldBe DataClass("test")
    }

    @Test
    fun `deserializeQueryResult can deserialize nested data class`() {
        data class Inner(val value: String)
        data class Outer(val inner: Inner)

        val innerSchema = FieldList.of(Field.newBuilder(Inner::value.name, LegacySQLTypeName.STRING).build())
        val innerAsRecordValue = FieldValue.of(FieldValue.Attribute.RECORD, FieldValueList.of(listOf(
            FieldValue.of(FieldValue.Attribute.PRIMITIVE, "test")
        )))
        val outerAsQueryResult = mapOf(
            Field.newBuilder(Outer::inner.name, LegacySQLTypeName.RECORD, innerSchema).build() to innerAsRecordValue
        )

        val result = BigQueryORMapping.deserializeQueryResult(listOf(outerAsQueryResult), Outer::class)

        result.single() shouldBe Outer(Inner("test"))
    }
}
