package rocks.frieler.kraftsql.bq.engine

import com.google.cloud.bigquery.Field
import com.google.cloud.bigquery.FieldValue
import com.google.cloud.bigquery.StandardSQLTypeName
import rocks.frieler.kraftsql.bq.expressions.Struct
import rocks.frieler.kraftsql.engine.ORMapping
import rocks.frieler.kraftsql.engine.ensuredPrimaryConstructor
import rocks.frieler.kraftsql.expressions.Expression
import rocks.frieler.kraftsql.expressions.Row
import rocks.frieler.kraftsql.objects.DataRow
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDate
import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.full.starProjectedType
import kotlin.reflect.jvm.jvmErasure
import kotlin.reflect.typeOf

object BigQueryORMapping : ORMapping<BigQueryEngine, Iterable<Map<Field, FieldValue>>> {
    override fun getTypeFor(type: KType) : Type<*> =
        when {
            type in setOf(typeOf<String>(), typeOf<String?>()) -> Types.STRING
            type in setOf(typeOf<Boolean>(), typeOf<Boolean?>()) -> Types.BOOL
            type in setOf(typeOf<Byte>(), typeOf<Byte?>()) -> Types.INT64
            type in setOf(typeOf<Short>(), typeOf<Short?>()) -> Types.INT64
            type in setOf(typeOf<Int>(), typeOf<Int?>()) -> Types.INT64
            type in setOf(typeOf<Long>(), typeOf<Long?>()) -> Types.INT64
            type in setOf(typeOf<Float>(), typeOf<Float?>()) -> Types.NUMERIC
            type in setOf(typeOf<Double>(), typeOf<Double?>()) -> Types.NUMERIC
            type in setOf(typeOf<BigDecimal>(), typeOf<BigDecimal?>()) -> Types.BIGNUMERIC
            type in setOf(typeOf<Instant>(), typeOf<Instant?>()) -> Types.TIMESTAMP
            type in setOf(typeOf<LocalDate>(), typeOf<LocalDate?>()) -> Types.DATE
            type.jvmErasure.starProjectedType == Array::class.starProjectedType -> Types.ARRAY(getTypeFor(type.arguments.single().type ?: Any::class.starProjectedType))
            type.jvmErasure.isData -> Types.STRUCT(type.jvmErasure.ensuredPrimaryConstructor().parameters.associate { param -> param.name!! to getTypeFor(param.type) })
            type in setOf(typeOf<DataRow>(), typeOf<DataRow?>()) -> throw NotImplementedError("BigQuery type for DataRow is not supported, as it would be STRUCT<?>, where DataRow does not provide information about it subfields.")
            else -> throw NotImplementedError("Unsupported Kotlin type $type")
        }

    override fun <T : Any> serialize(value: T?): Expression<BigQueryEngine, T> {
        fun <T : Any> replaceWithBQExpressions(expression: Expression<BigQueryEngine, T>) : Expression<BigQueryEngine, T> =
            when (expression) {
                is rocks.frieler.kraftsql.expressions.Constant -> rocks.frieler.kraftsql.bq.expressions.Constant(expression.value)
                is Row -> Struct(
                    expression.values?.mapValues { (_, value) ->
                        @Suppress("UNCHECKED_CAST")
                        replaceWithBQExpressions(value as Expression<BigQueryEngine, Any>)
                    })
                else -> expression
            }

        return replaceWithBQExpressions(super.serialize(value))
    }

    override fun <T : Any> deserializeQueryResult(queryResult: Iterable<Map<Field, FieldValue>>, type: KClass<T>): List<T> =
        queryResult.map { row -> deserializeRow(row, type) }

    private fun <T : Any> deserializeRow(row: Map<Field, FieldValue>, type: KClass<T>): T =
        when {
        type == Integer::class -> {
            @Suppress("UNCHECKED_CAST")
            row.values.single().longValue.toInt() as T
        }
        type == Long::class -> {
            @Suppress("UNCHECKED_CAST")
            row.values.single().longValue as T
        }
        type == Float::class -> {
            @Suppress("UNCHECKED_CAST")
            row.values.single().doubleValue.toFloat() as T
        }
        type == Double::class -> {
            @Suppress("UNCHECKED_CAST")
            row.values.single().doubleValue as T
        }
        type == BigDecimal::class -> {
            @Suppress("UNCHECKED_CAST")
            row.values.single().numericValue as T
        }
        type == String::class -> {
            @Suppress("UNCHECKED_CAST")
            row.values.single().stringValue as T
        }
        type == Instant::class -> {
            @Suppress("UNCHECKED_CAST")
            row.values.single().timestampInstant as T
        }
        type.starProjectedType == typeOf<Array<*>>() -> {
            val field = row.keys.single()
            val fieldValues = row.values.single().repeatedValue
            val elementField = Field.of("_", field.type.standardType, field.subFields)
            val elementType = elementField.getSqlType().naturalType.jvmErasure
            val elements = deserializeQueryResult(fieldValues.map { value -> mapOf(elementField to value) }, elementType)
            val array = java.lang.reflect.Array.newInstance(elementType.java, elements.size)
            @Suppress("UNCHECKED_CAST")
            elements.forEachIndexed { index, element -> (array as Array<Any?>)[index] = element }
            @Suppress("UNCHECKED_CAST")
            array as T
        }
        type == DataRow::class -> {
            @Suppress("UNCHECKED_CAST")
            DataRow(row.entries.associate { (field, fieldValue) ->
                if (field.subFields.isNullOrEmpty()) {
                    field.name to deserializeRow(mapOf(field to fieldValue), field.getSqlType().naturalType.jvmErasure)
                } else {
                    field.name to deserializeRow(field.subFields.associateWith { fieldValue.recordValue.get(it.name) }, field.getSqlType().naturalType.jvmErasure)
                }
            }) as T
        }
        type.isData -> {
            val constructor = type.ensuredPrimaryConstructor()
            constructor.callBy(constructor.parameters.associateWith { param ->
                val field = row.keys.single { it.name == param.name }
                val fieldValue = row[field]!!
                deserializeRow(mapOf(field to fieldValue), param.type.jvmErasure)
            })
        }
        else -> throw IllegalArgumentException("Unsupported target type ${type.qualifiedName}.")
    }

    private fun Field.getSqlType() : Type<*> {
        return when {
            this.mode == Field.Mode.REPEATED -> {
                if (type.standardType == StandardSQLTypeName.STRUCT) {
                    Types.ARRAY(Types.STRUCT(this.subFields.associate { subfield -> subfield.name to subfield.getSqlType() }))
                } else {
                    Types.ARRAY(Types.parseType(type.standardType.name))
                }
            }
            this.type.standardType == StandardSQLTypeName.STRUCT -> {
                Types.STRUCT(subFields.associate { subfield -> subfield.name to subfield.getSqlType() })
            }
            else -> Types.parseType(this.type.standardType.name)
        }
    }
}
