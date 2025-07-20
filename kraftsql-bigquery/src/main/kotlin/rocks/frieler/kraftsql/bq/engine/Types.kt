package rocks.frieler.kraftsql.bq.engine

import com.google.cloud.bigquery.StandardSQLTypeName

val STRING = Type(StandardSQLTypeName.STRING)

val BOOL = Type(StandardSQLTypeName.BOOL)

val INT64 = Type(StandardSQLTypeName.INT64)

val NUMERIC = Type(StandardSQLTypeName.NUMERIC)

val TIMESTAMP = Type(StandardSQLTypeName.TIMESTAMP)

// TODO: implement all types (https://cloud.google.com/bigquery/docs/reference/standard-sql/data-types)
