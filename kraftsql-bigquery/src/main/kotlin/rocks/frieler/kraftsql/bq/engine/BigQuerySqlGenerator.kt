package rocks.frieler.kraftsql.bq.engine

import rocks.frieler.kraftsql.data.ConstantData
import rocks.frieler.kraftsql.data.DataRow
import rocks.frieler.kraftsql.engine.DataClassSerDe

/**
 * Utility object that generates BigQuery SQL for KraftSQL [rocks.frieler.kraftsql.data.Data].
 */
object BigQuerySqlGenerator {

    /**
     * Generates a `SELECT` statement to select the given [ConstantData] in BigQuery.
     *
     * @param data the [ConstantData] to select
     * @return a `SELECT` statement to select the `data`
     */
    fun makeSQL(data: ConstantData<*>) : String {
        val rows = data.items.map { it as? DataRow ?: DataClassSerDe.serialize(it) }

        var orderingColumn = "row_number"
        while (orderingColumn in data.schema.map { it.name }) {
            orderingColumn = "_$orderingColumn"
        }
        return """
            SELECT ${data.schema.joinToString(separator = ",") { "`${it.name}`" }}
            FROM UNNEST(ARRAY<STRUCT<${data.schema.joinToString(separator = ",") { "`${it.name}` ${it.type.native.name}" }}>>[
              ${rows.joinToString(separator = ",") { row ->
                  row.entries.joinToString(separator = ",", prefix = "STRUCT(", postfix = ")") { entry -> constant(entry.second) }
              }}
            ]) WITH OFFSET AS $orderingColumn
            ORDER BY $orderingColumn
        """.trimIndent()
    }

    internal fun constant(value: Any?) = when (value) {
        null -> "NULL"
        is String -> "'${value
            .replace("\\", "\\\\")
            .replace("'", "\\'")
            .replace("\n", "\\n")
        }'"
        else -> value.toString()
    }
}
