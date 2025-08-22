package rocks.frieler.kraftsql.bq.engine

import com.google.cloud.bigquery.Field
import com.google.cloud.bigquery.FieldValue
import com.google.cloud.bigquery.FieldValueList
import rocks.frieler.kraftsql.engine.ORMapping
import rocks.frieler.kraftsql.objects.DataRow
import java.time.Instant
import kotlin.reflect.KClass
import kotlin.reflect.KType
import kotlin.reflect.KTypeProjection
import kotlin.reflect.full.createType
import kotlin.reflect.full.starProjectedType
import kotlin.reflect.jvm.jvmErasure
import kotlin.reflect.typeOf

object BigQueryORMapping : ORMapping<BigQueryEngine, Iterable<Map<Field, FieldValue>>> {
    override fun getTypeFor(type: KType) : Type =
        when (type.jvmErasure.starProjectedType) {
            String::class.starProjectedType -> Types.STRING
            Boolean::class.starProjectedType -> Types.BOOL
            Byte::class.starProjectedType -> Types.INT64
            Short::class.starProjectedType -> Types.INT64
            Int::class.starProjectedType -> Types.INT64
            Long::class.starProjectedType -> Types.INT64
            Float::class.starProjectedType -> Types.NUMERIC
            Double::class.starProjectedType -> Types.NUMERIC
            Instant::class.starProjectedType -> Types.TIMESTAMP
            Array::class.starProjectedType -> Types.ARRAY(getTypeFor(type.arguments.single().type ?: Any::class.starProjectedType))
            else -> throw NotImplementedError("Unsupported Kotlin type $type")
        }

    override fun getKTypeFor(sqlType: rocks.frieler.kraftsql.engine.Type<BigQueryEngine>) : KType =
        when (sqlType) {
            Types.STRING -> typeOf<String>()
            Types.BOOL -> typeOf<Boolean>()
            Types.INT64 -> typeOf<Long>()
            Types.NUMERIC -> typeOf<Double>()
            Types.TIMESTAMP -> typeOf<Instant>()
            is Types.ARRAY -> Array::class.createType(listOf(KTypeProjection.invariant(getKTypeFor(sqlType.contentType))))
            else -> throw NotImplementedError("Unsupported SQL type $sqlType")
        }

    override fun <T : Any> deserializeQueryResult(queryResult: Iterable<Map<Field, FieldValue>>, type: KClass<T>): List<T> {
        return queryResult.map { row ->
            if (type == DataRow::class) {
                @Suppress("UNCHECKED_CAST")
                DataRow(row.entries.associate { (field, fieldValue) -> field.name to
                        fieldValue.value.let { value ->
                        when (value) {
                            is FieldValueList -> {
                                if (field.mode == Field.Mode.REPEATED) {
                                    val elementType = getKTypeFor(Types.parseType(field.type.standardType.name)).jvmErasure.java
                                    (java.lang.reflect.Array.newInstance(elementType, value.size) as Array<Any?>).also { array ->
                                        value.forEachIndexed { index, element -> array[index] = element.value }
                                    }
                                } else {
                                    NotImplementedError("Other FieldValueLists then REPEATED fields are not implemented yet.")
                                }
                            }
                            else -> value
                        }
                    }
                }) as T
            } else {
                val constructor = type.constructors.first()
                val fieldValues = row.mapKeys { (field, _) -> field.name }
                constructor.callBy(constructor.parameters.associateWith { param ->
                    when (param.type.jvmErasure.starProjectedType) {
                        Integer::class.starProjectedType -> fieldValues[param.name]!!.numericValue.toInt()
                        Long::class.starProjectedType -> fieldValues[param.name]!!.numericValue.toLong()
                        String::class.starProjectedType -> fieldValues[param.name]!!.stringValue
                        Array::class.starProjectedType -> {
                            val elements = fieldValues[param.name]!!.repeatedValue.map { element -> element.value }
                            val elementType = param.type.arguments.single().type!!.jvmErasure
                            @Suppress("UNCHECKED_CAST")
                            val array = java.lang.reflect.Array.newInstance(elementType.java, elements.size) as Array<Any?>
                            elements.forEachIndexed { index, element -> array[index] = element }
                            return@associateWith array
                        }
                        else -> throw NotImplementedError("Unsupported field type ${param.type}")
                    }
                })
            }
        }
    }
}
