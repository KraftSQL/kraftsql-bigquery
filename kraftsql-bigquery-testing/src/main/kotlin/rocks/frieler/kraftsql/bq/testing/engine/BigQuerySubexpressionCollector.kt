package rocks.frieler.kraftsql.bq.testing.engine

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.expressions.JsonValue
import rocks.frieler.kraftsql.bq.expressions.JsonValueArray
import rocks.frieler.kraftsql.bq.expressions.Replace
import rocks.frieler.kraftsql.bq.expressions.Timestamp
import rocks.frieler.kraftsql.expressions.Expression
import rocks.frieler.kraftsql.testing.engine.GenericSubexpressionCollector

/**
 * [GenericSubexpressionCollector] for [Expression]s supported by BigQuery.
 */
class BigQuerySubexpressionCollector : GenericSubexpressionCollector<BigQueryEngine>() {
    override fun getSubexpressions(expression: Expression<BigQueryEngine, *>): List<Expression<BigQueryEngine, Any?>> =
        when (expression) {
            is JsonValue -> listOfNotNull(expression.jsonString, expression.jsonPath)
            is JsonValueArray -> listOfNotNull(expression.jsonString, expression.jsonPath)
            is Replace -> listOf(expression.originalValue, expression.fromPattern, expression.toPattern)
            is Timestamp -> listOf(expression.stringExpression)
            else -> super.getSubexpressions(expression)
        }
}
