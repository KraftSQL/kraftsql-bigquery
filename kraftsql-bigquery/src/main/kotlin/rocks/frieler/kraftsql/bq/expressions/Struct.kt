package rocks.frieler.kraftsql.bq.expressions

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.expressions.Expression
import rocks.frieler.kraftsql.expressions.Row

class Struct<T : Any>(values: Map<String, Expression<BigQueryEngine, *>>?) : Row<BigQueryEngine, T>(values) {
    override fun sql(): String {
        if (values == null) {
            return "NULL"
        }
        return "STRUCT(${values!!.values.joinToString(", ") { value -> value.sql() }})"
    }
}
