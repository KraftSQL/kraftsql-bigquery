package rocks.frieler.kraftsql.bq.dsl

import rocks.frieler.kraftsql.dsl.SelectBuilder
import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.dsl.Select
import rocks.frieler.kraftsql.dsl.SqlDsl

fun <T : Any> Select(configurator: @SqlDsl SelectBuilder<BigQueryEngine, T>.() -> Unit) = Select<BigQueryEngine, T>(configurator)
