package rocks.frieler.kraftsql.bq.dql

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.expressions.Expression
import rocks.frieler.kraftsql.dql.Join
import rocks.frieler.kraftsql.dql.Projection
import rocks.frieler.kraftsql.dql.QuerySource
import rocks.frieler.kraftsql.dql.Select
import rocks.frieler.kraftsql.dql.execute

class Select<T : Any> : Select<BigQueryEngine, T> {
    constructor(
        source: QuerySource<BigQueryEngine, *>,
        joins: List<Join<BigQueryEngine>> = emptyList(),
        columns: List<Projection<BigQueryEngine, *>>? = null,
        filter: Expression<BigQueryEngine, Boolean>? = null,
        grouping: List<Expression<BigQueryEngine, *>> = emptyList(),
    ) : super(source, joins, columns, filter, grouping)
}

inline fun <reified T : Any> Select<BigQueryEngine, T>.execute() =
    execute(BigQueryEngine.DefaultConnection.get())
