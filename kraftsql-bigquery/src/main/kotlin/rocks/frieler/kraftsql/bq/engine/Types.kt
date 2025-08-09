package rocks.frieler.kraftsql.bq.engine

import com.google.cloud.bigquery.StandardSQLTypeName

object Types {

    val STRING = Type(StandardSQLTypeName.STRING)

    val BOOL = Type(StandardSQLTypeName.BOOL)

    val INT64 = Type(StandardSQLTypeName.INT64)

    val NUMERIC = Type(StandardSQLTypeName.NUMERIC)

    val TIMESTAMP = Type(StandardSQLTypeName.TIMESTAMP)

    class ARRAY(val contentType: Type) : Type(StandardSQLTypeName.ARRAY) { override fun sql() = "ARRAY<${contentType.sql()}>" }

    // TODO: implement all types (https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types)

    fun parseType(type: String) : Type = when {
        type == STRING.name.name -> STRING
        type == BOOL.name.name -> BOOL
        type == INT64.name.name -> INT64
        type == NUMERIC.name.name -> NUMERIC
        type == TIMESTAMP.name.name -> TIMESTAMP
        type.matches("^ARRAY<.+>$".toRegex()) -> ARRAY(parseType(type.removePrefix("ARRAY<").removeSuffix(">")))
        else -> error("unknown BigQuery type: '$type'")
    }
}
