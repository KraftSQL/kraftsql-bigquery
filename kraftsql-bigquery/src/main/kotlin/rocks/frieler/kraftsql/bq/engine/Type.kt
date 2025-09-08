package rocks.frieler.kraftsql.bq.engine

import com.google.cloud.bigquery.StandardSQLTypeName
import rocks.frieler.kraftsql.engine.Type
import kotlin.reflect.KType

open class Type<T : Any>(
    val name: StandardSQLTypeName,
    val naturalType: KType,
) : Type<BigQueryEngine, T> {
    override fun sql() = name.name
    override fun naturalKType() = naturalType
}
