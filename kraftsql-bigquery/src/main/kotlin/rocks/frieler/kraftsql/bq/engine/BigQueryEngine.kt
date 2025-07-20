package rocks.frieler.kraftsql.bq.engine

import rocks.frieler.kraftsql.engine.Engine
import java.time.Instant
import kotlin.reflect.KType
import kotlin.reflect.full.starProjectedType

object BigQueryEngine : Engine<BigQueryEngine> {
    override fun getTypeFor(type: KType) =
        when (type) {
            String::class.starProjectedType -> STRING
            Boolean::class.starProjectedType -> BOOL
            Byte::class.starProjectedType -> INT64
            Short::class.starProjectedType -> INT64
            Int::class.starProjectedType -> INT64
            Long::class.starProjectedType -> INT64
            Float::class.starProjectedType -> NUMERIC
            Double::class.starProjectedType -> NUMERIC
            Instant::class.starProjectedType -> TIMESTAMP
            else -> throw NotImplementedError("Unsupported Kotlin type $type")
        }
}
