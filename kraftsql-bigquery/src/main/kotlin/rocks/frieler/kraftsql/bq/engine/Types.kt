package rocks.frieler.kraftsql.bq.engine

import com.google.cloud.bigquery.StandardSQLTypeName
import rocks.frieler.kraftsql.objects.Column
import rocks.frieler.kraftsql.objects.DataRow
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDate
import kotlin.reflect.KTypeProjection
import kotlin.reflect.full.createType
import kotlin.reflect.typeOf

/**
 * The [Type]s of BigQuery.
 *
 * See [Standard SQL data types](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types).
 */
object Types {

    val STRING = Type<String>(StandardSQLTypeName.STRING, typeOf<String>())

    val BOOL = Type<Boolean>(StandardSQLTypeName.BOOL, typeOf<Boolean>())

    val INT64 = Type<Long>(StandardSQLTypeName.INT64, typeOf<Long>())

    val NUMERIC = Type<Double>(StandardSQLTypeName.NUMERIC, typeOf<Double>())

    val BIGNUMERIC = Type<BigDecimal>(StandardSQLTypeName.BIGNUMERIC, typeOf<BigDecimal>())

    val TIMESTAMP = Type<Instant>(StandardSQLTypeName.TIMESTAMP, typeOf<Instant>())

    val DATE = Type<LocalDate>(StandardSQLTypeName.DATE, typeOf<LocalDate>())

    class ARRAY<C : Any>(val contentType: Type<C>) : Type<Array<C>>(StandardSQLTypeName.ARRAY, typeOf<Array<*>>()) {
        override fun sql() = "ARRAY<${contentType.sql()}>"
        override fun naturalKType() = Array::class.createType(listOf(KTypeProjection.invariant(contentType.naturalType)))

        companion object {
            val matcher = "^ARRAY<.+>$".toRegex()

            fun parse(type: String): ARRAY<*> = ARRAY(parseType(type.removePrefix("ARRAY<").removeSuffix(">")))
        }
    }

    /**
     * BigQuery's [STRUCT type](https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types#struct_type).
     *
     * @param fields the subfields of the STRUCT
     */
    class STRUCT(val fields: List<Column<BigQueryEngine>>) : Type<DataRow>(StandardSQLTypeName.STRUCT, typeOf<DataRow>()) {
        override fun sql() = "STRUCT<${fields.joinToString(",") { column -> "${column.name} ${column.type.sql()}"} }>"

        companion object {
            val matcher = "^STRUCT<.+>$".toRegex()

            fun parse(type: String) = STRUCT(
                type.removePrefix("STRUCT<").removeSuffix(">")
                    .split(",") // FIXME: handle nested structs
                    .map { it.trim().split(" +".toRegex(), limit = 2) }
                    .map { (name, type) -> Column(name, parseType(type), nullable = true) } // TODO: can we detect non-nullable sub-fields?
            )
        }
    }

    // TODO: implement all types (https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types)

    fun parseType(type: String) : Type<*> = when {
        type == STRING.name.name -> STRING
        type == BOOL.name.name -> BOOL
        type == INT64.name.name -> INT64
        type == NUMERIC.name.name -> NUMERIC
        type == BIGNUMERIC.name.name -> BIGNUMERIC
        type == TIMESTAMP.name.name -> TIMESTAMP
        type == DATE.name.name -> DATE
        type.matches(ARRAY.matcher) -> ARRAY.parse(type)
        type.matches(STRUCT.matcher) -> STRUCT.parse(type)
        else -> error("unknown BigQuery type: '$type'")
    }
}
