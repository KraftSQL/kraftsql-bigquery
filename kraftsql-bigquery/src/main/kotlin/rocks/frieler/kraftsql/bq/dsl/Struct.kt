package rocks.frieler.kraftsql.bq.dsl

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.dql.Projection

fun <T : Any> Struct(vararg fields: Projection<BigQueryEngine, *>) = rocks.frieler.kraftsql.bq.expressions.Struct<T>(
    fields.associate { field -> (field.alias ?: throw IllegalArgumentException("Struct fields must be named.")) to field.value })
