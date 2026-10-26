package rocks.frieler.kraftsql.bq.engine

import rocks.frieler.kraftsql.data.ConstantData
import rocks.frieler.kraftsql.data.Data
import rocks.frieler.kraftsql.data.DataRow
import rocks.frieler.kraftsql.data.Selection
import rocks.frieler.kraftsql.engine.DataClassSerDe
import rocks.frieler.kraftsql.expressions.Column

/**
 * Utility object that generates BigQuery SQL for KraftSQL [rocks.frieler.kraftsql.data.Data].
 */
object BigQuerySqlGenerator {
    /**
     * Generates a `SELECT` statement to select the given [Data] in BigQuery.
     *
     * @param data the [Data] to select
     * @return a `SELECT` statement to select the `data`
     */
    fun makeSQL(data: Data<*>): String = when (data) {
        is ConstantData<*> -> makeSQL(data)
        is Selection -> makeSQL(data)
        else -> throw NotImplementedError("Collecting $data is not implemented.")
    }

    private fun makeSQL(data: ConstantData<*>) : String {
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

    private fun makeSQL(data: Selection): String = """
        SELECT ${data.expressions.joinToString(separator = ",") { "${column(it.expression as Column<*>)} AS `${it.name}`" }}
        FROM (${makeSQL(data.source)})
    """.trimIndent() // TODO: Merge multiple Selections.

    internal fun constant(value: Any?) = when (value) {
        null -> "NULL"
        is String -> "'${value
            .replace("\\", "\\\\")
            .replace("'", "\\'")
            .replace("\n", "\\n")
        }'"
        else -> value.toString()
    }

    internal fun column(column: Column<*>) = "`${column.name}`"
}
