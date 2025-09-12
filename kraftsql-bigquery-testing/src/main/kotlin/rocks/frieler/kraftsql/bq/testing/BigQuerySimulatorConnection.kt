package rocks.frieler.kraftsql.bq.testing

import com.jayway.jsonpath.JsonPath
import rocks.frieler.kraftsql.bq.engine.BigQueryConnection
import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.expressions.JsonValue
import rocks.frieler.kraftsql.bq.expressions.JsonValueArray
import rocks.frieler.kraftsql.bq.expressions.Replace
import rocks.frieler.kraftsql.bq.expressions.Timestamp
import rocks.frieler.kraftsql.bq.objects.TemporaryTable
import rocks.frieler.kraftsql.ddl.CreateTable
import rocks.frieler.kraftsql.ddl.DropTable
import rocks.frieler.kraftsql.dml.BeginTransaction
import rocks.frieler.kraftsql.dml.Delete
import rocks.frieler.kraftsql.dml.InsertInto
import rocks.frieler.kraftsql.dml.RollbackTransaction
import rocks.frieler.kraftsql.dql.Select
import rocks.frieler.kraftsql.expressions.Expression
import rocks.frieler.kraftsql.objects.DataRow
import rocks.frieler.kraftsql.objects.Table
import rocks.frieler.kraftsql.testing.engine.EngineState
import rocks.frieler.kraftsql.testing.engine.GenericSimulatorConnection
import java.time.Instant
import kotlin.reflect.KClass

class BigQuerySimulatorConnection : BigQueryConnection, GenericSimulatorConnection<BigQueryEngine>() {
    private val timestampLiteralPattern = "^(?<date>\\d{4}-\\d{1,2}-\\d{1,2})[Tt ](?<time>\\d{1,2}:\\d{1,2}:\\d{1,2}(.\\d{1,6})?)?(?<tz>|[Zz]|[+-]\\d{1,2}(:\\d{2})?| .+/.+)$".toPattern()

    private var sessionMode = false
    private var activeSession : SessionState? = null

    private class SessionState(
        private val parent: EngineState<BigQueryEngine>,
    ): EngineState<BigQueryEngine>() {
        override fun containsTable(name: String): Boolean {
            return super.containsTable(name) || parent.containsTable(name)
        }

        override fun findTable(name: String): Pair<Table<BigQueryEngine, *>, MutableList<DataRow>>? {
            return super.findTable(name) ?: parent.findTable(name)
        }

        override fun addTable(table: Table<BigQueryEngine, *>) {
            if (table is TemporaryTable<*>) {
                super.addTable(table)
            } else {
                parent.addTable(table)
            }
        }

        override fun removeTable(table: Table<BigQueryEngine, *>) {
            if (table is TemporaryTable<*>) {
                super.removeTable(table)
            } else {
                parent.removeTable(table)
            }
        }

        override fun writeTable(table: Table<BigQueryEngine, *>, data: List<DataRow>) {
            if (table is TemporaryTable<*>) {
                super.writeTable(table, data)
            } else {
                parent.writeTable(table, data)
            }
        }
    }

    override fun <T : Any> execute(select: Select<BigQueryEngine, T>, type: KClass<T>): List<T> {
        if (sessionMode) { ensureSession() }
        return super.execute(select, type)
    }

    override fun execute(createTable: CreateTable<BigQueryEngine>) {
        if (createTable.table is TemporaryTable<*>) {
            throw UnsupportedOperationException("The BigQuery API does not support creation of temporary tables.")
        }
        super.execute(createTable)
    }

    override fun execute(dropTable: DropTable<BigQueryEngine>) {
        if (dropTable.table is TemporaryTable<*>) {
            throw UnsupportedOperationException("The BigQuery API does not support dropping temporary tables.")
        }
        super.execute(dropTable)
    }

    override fun execute(insertInto: InsertInto<BigQueryEngine, *>): Int {
        if (sessionMode) { ensureSession() }
        return super.execute(insertInto)
    }

    override fun execute(delete: Delete<BigQueryEngine>): Int {
        if (sessionMode) { ensureSession() }
        return super.execute(delete)
    }

    override fun execute(beginTransaction: BeginTransaction<BigQueryEngine>) {
        check(topState !is TransactionStateOverlay<BigQueryEngine>) { "There is already an open transaction and nested transactions are not supported." }
        check(sessionMode) { "Beginning a transaction would require a session, but session mode is turned off." }
            .also { ensureSession() }
        super.execute(beginTransaction)
    }

    override fun setSessionMode(sessionMode: Boolean) {
        if (!sessionMode && activeSession != null) {
            if (topState is TransactionStateOverlay<BigQueryEngine>) { execute(RollbackTransaction()) }
            activeSession = null
        }
        this.sessionMode = sessionMode
    }

    private fun ensureSession() {
        activeSession = activeSession ?: SessionState(rootState).also { topState = it }
    }

    override fun <T> simulateExpression(expression: Expression<BigQueryEngine, T>) : (DataRow) -> T? =
        when (expression) {
            is Replace -> { row ->
                val originalValue = simulateExpression(expression.originalValue).invoke(row)
                val fromPattern = simulateExpression(expression.fromPattern).invoke(row)!!
                @Suppress("UNCHECKED_CAST")
                if (fromPattern.isEmpty()) {
                    originalValue
                } else {
                    val toPattern = simulateExpression(expression.toPattern).invoke(row)!!
                    originalValue?.replace(fromPattern, toPattern)
                } as T?
            }
            is Timestamp -> { row ->
                val timestamp = simulateExpression(expression.stringExpression).invoke(row)
                @Suppress("UNCHECKED_CAST")
                timestamp?.let {
                    val matcher = timestampLiteralPattern.matcher(it)
                    if (!matcher.matches()) {
                        throw IllegalArgumentException("invalid timestamp format: $it")
                    }
                    val canonicalTimestamp = "${matcher.group("date")}" +
                            "T${matcher.group("time") ?: "00:00:00.000000"}" +
                            (matcher.group("tz").trim().ifEmpty { null } ?: "Z")
                    Instant.parse(canonicalTimestamp)
                } as T?
            }
            is JsonValue -> { row ->
                val jsonString = simulateExpression(expression.jsonString).invoke(row)
                val jsonPath = expression.jsonPath?.let { simulateExpression(it).invoke(row) }
                @Suppress("UNCHECKED_CAST")
                JsonPath.read<String>(jsonString, jsonPath ?: "$") as T?
            }
            is JsonValueArray -> { row ->
                val jsonString = simulateExpression(expression.jsonString).invoke(row).let { if (it.isNullOrBlank()) "[]" else it }
                val jsonPath = expression.jsonPath?.let { simulateExpression(it).invoke(row) }
                @Suppress("UNCHECKED_CAST")
                JsonPath.read<List<Any>>(jsonString, jsonPath ?: "$").map { it.toString() }.toTypedArray() as T?
            }
            else -> super.simulateExpression(expression)
        }
}
