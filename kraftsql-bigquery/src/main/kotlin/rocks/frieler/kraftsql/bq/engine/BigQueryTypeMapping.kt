package rocks.frieler.kraftsql.bq.engine

import com.google.cloud.bigquery.StandardSQLTypeName
import rocks.frieler.kraftsql.data.Type

/**
 * The native BigQuery [StandardSQLTypeName] this KraftSQL [Type] gets mapped to.
 *
 * - [Int][rocks.frieler.kraftsql.data.Type.INTEGER] to `INT64`
 * - [String][rocks.frieler.kraftsql.data.Type.STRING] to `STRING`
 */
val Type.native get() = when (this) {
    Type.INTEGER, Type.NULLABLE_INTEGER -> StandardSQLTypeName.INT64
    Type.STRING, Type.NULLABLE_STRING -> StandardSQLTypeName.STRING
    else -> throw IllegalStateException("Unsupported type $this")
}
