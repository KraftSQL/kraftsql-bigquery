package rocks.frieler.kraftsql.bq.engine

import com.google.cloud.bigquery.StandardSQLTypeName

object Types {

    val STRING = Type(StandardSQLTypeName.STRING)

    val BOOL = Type(StandardSQLTypeName.BOOL)

    val INT64 = Type(StandardSQLTypeName.INT64)

    val NUMERIC = Type(StandardSQLTypeName.NUMERIC)

    val BIGNUMERIC = Type(StandardSQLTypeName.BIGNUMERIC)

    val TIMESTAMP = Type(StandardSQLTypeName.TIMESTAMP)

    class ARRAY(val contentType: Type) : Type(StandardSQLTypeName.ARRAY) {
        override fun sql() = "ARRAY<${contentType.sql()}>"

        companion object {
            val matcher = "^ARRAY<.+>$".toRegex()

            fun parse(type: String) = ARRAY(parseType(type.removePrefix("ARRAY<").removeSuffix(">")))
        }
    }

    class STRUCT(val fields: Map<String, Type>) : Type(StandardSQLTypeName.STRUCT) {
        override fun sql() = "STRUCT<${fields.entries.joinToString(",") { (name, type) -> "$name ${type.sql()}"} }>"

        companion object {
            val matcher = "^STRUCT<.+>$".toRegex()

            fun parse(type: String) = STRUCT(
                type.removePrefix("STRUCT<").removeSuffix(">")
                    .split(",") // FIXME: handle nested structs
                    .map { it.trim().split(" +".toRegex(), limit = 2) }
                    .associate { (name, type) -> name to parseType(type) }
            )
        }
    }

    // TODO: implement all types (https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types)

    fun parseType(type: String) : Type = when {
        type == STRING.name.name -> STRING
        type == BOOL.name.name -> BOOL
        type == INT64.name.name -> INT64
        type == NUMERIC.name.name -> NUMERIC
        type == BIGNUMERIC.name.name -> BIGNUMERIC
        type == TIMESTAMP.name.name -> TIMESTAMP
        type.matches(ARRAY.matcher) -> ARRAY.parse(type)
        type.matches(STRUCT.matcher) -> STRUCT.parse(type)
        else -> error("unknown BigQuery type: '$type'")
    }
}
