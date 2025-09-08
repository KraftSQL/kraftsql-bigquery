package rocks.frieler.kraftsql.bq.expressions

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.expressions.Expression
import java.util.Objects

class Replace(
    val originalValue: Expression<BigQueryEngine, String>,
    val fromPattern: Expression<BigQueryEngine, String>,
    val toPattern: Expression<BigQueryEngine, String>,
) : Expression<BigQueryEngine, String> {

    override fun sql() =
        "REPLACE(${originalValue.sql()}, ${fromPattern.sql()}, ${toPattern.sql()})"

    override fun defaultColumnName() =
        "REPLACE(${originalValue.defaultColumnName()}, ${fromPattern.defaultColumnName()}, ${toPattern.defaultColumnName()})"

    override fun equals(other: Any?) =
        other is Replace
                && originalValue == other.originalValue
                && fromPattern == other.fromPattern
                && toPattern == other.toPattern

    override fun hashCode() =
        Objects.hash(originalValue, fromPattern, toPattern)
}
