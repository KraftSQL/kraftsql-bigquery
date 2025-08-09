package rocks.frieler.kraftsql.bq.engine

import com.google.cloud.bigquery.StandardSQLTypeName
import rocks.frieler.kraftsql.engine.Type

open class Type(
    val name: StandardSQLTypeName,
) : Type<BigQueryEngine> {
    override fun sql() = name.name
}
