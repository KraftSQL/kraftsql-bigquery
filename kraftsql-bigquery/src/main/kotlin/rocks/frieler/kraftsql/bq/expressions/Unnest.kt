package rocks.frieler.kraftsql.bq.expressions

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.objects.Data
import rocks.frieler.kraftsql.expressions.Expression
import rocks.frieler.kraftsql.objects.HasColumns

class Unnest<T : Any>(
    val arrayExpression: Expression<BigQueryEngine, Array<out T>?>,
) : Expression<BigQueryEngine, Data<T>>, HasColumns<BigQueryEngine, T> {
    override fun defaultColumnName() = arrayExpression.defaultColumnName()

    override val columnNames = listOf("")

    override val subexpressions = listOf(arrayExpression)

    override fun sql() = "UNNEST(${arrayExpression.sql()})"

    override fun equals(other: Any?): Boolean {
        TODO("Not yet implemented")
    }

    override fun hashCode(): Int {
        TODO("Not yet implemented")
    }
}
