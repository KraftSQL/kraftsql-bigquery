package rocks.frieler.kraftsql.bq.dml

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.objects.FileSource
import rocks.frieler.kraftsql.bq.objects.Table
import rocks.frieler.kraftsql.bq.objects.TemporaryTable
import rocks.frieler.kraftsql.commands.Command
import rocks.frieler.kraftsql.objects.Column

class LoadData(
    val overwrite: Boolean = false,
    val table: Table<*>,
    val columns: List<Column<BigQueryEngine>>? = null,
    val fileSource: FileSource,
) : Command<BigQueryEngine, Unit> {
    override fun sql(): String {
        return """
            LOAD DATA
            ${ if (overwrite) "OVERWRITE" else "INTO" }${if (table is TemporaryTable) " TEMPORARY TABLE" else ""} ${table.sql()}
            ${ if (columns != null) "(${columns.joinToString(", ") { it.sql() }})" else ""}
            FROM FILES(${fileSource.toOptions().entries.joinToString(",") { (key, value) -> "${key}=$value" }})
        """.trimIndent().replace("\n\\s*\n".toRegex(), "\n")
    }
}

fun LoadData.execute() = BigQueryEngine.DefaultConnection.get().execute(this)
