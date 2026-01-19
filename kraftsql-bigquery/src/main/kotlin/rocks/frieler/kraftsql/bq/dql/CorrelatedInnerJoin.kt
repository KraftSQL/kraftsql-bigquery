package rocks.frieler.kraftsql.bq.dql

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.objects.Data
import rocks.frieler.kraftsql.dql.Join
import rocks.frieler.kraftsql.dql.Projection
import rocks.frieler.kraftsql.dql.QuerySource

class CorrelatedInnerJoin<T : Any>(
    val expression: Projection<BigQueryEngine, Data<T>>,
) : Join<BigQueryEngine>(QuerySource(CorrelatedJoinData(expression))) {
    override fun sql() = "INNER JOIN ${data.sql()}"
}

private class CorrelatedJoinData<T : Any>(
    val expression: Projection<BigQueryEngine, Data<T>>,
) : Data<T> {

    override val columnNames = listOf(expression.alias ?: expression.value.defaultColumnName())

    override fun sql() = expression.sql()
}
