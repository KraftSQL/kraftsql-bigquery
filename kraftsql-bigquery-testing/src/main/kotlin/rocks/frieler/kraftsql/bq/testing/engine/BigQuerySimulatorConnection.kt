package rocks.frieler.kraftsql.bq.testing.engine

import org.apache.commons.csv.CSVFormat
import rocks.frieler.kraftsql.bq.dml.LoadData
import rocks.frieler.kraftsql.bq.engine.BigQueryConnection
import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.objects.TemporaryTable
import rocks.frieler.kraftsql.ddl.CreateTable
import rocks.frieler.kraftsql.ddl.DropTable
import rocks.frieler.kraftsql.dml.BeginTransaction
import rocks.frieler.kraftsql.dml.Delete
import rocks.frieler.kraftsql.dml.InsertInto
import rocks.frieler.kraftsql.dml.RollbackTransaction
import rocks.frieler.kraftsql.dql.Select
import rocks.frieler.kraftsql.objects.DataRow
import rocks.frieler.kraftsql.objects.Table
import rocks.frieler.kraftsql.testing.engine.EngineState
import rocks.frieler.kraftsql.testing.engine.GenericSimulatorConnection
import java.io.FileReader
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDate
import kotlin.reflect.KClass
import kotlin.reflect.typeOf

/**
 * [rocks.frieler.kraftsql.testing.engine.SimulatorConnection] for the [BigQueryEngine].
 */
class BigQuerySimulatorConnection : BigQueryConnection, GenericSimulatorConnection<BigQueryEngine>(orm = BigQuerySimulatorORMapping) {
    private var sessionMode = false
    private var activeSession: SessionState? = null

    private class SessionState(
        private val parent: EngineState<BigQueryEngine>,
    ) : EngineState<BigQueryEngine>() {
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
        if (sessionMode) {
            ensureSession()
        }
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
        if (sessionMode) {
            ensureSession()
        }
        return super.execute(insertInto)
    }

    override fun execute(delete: Delete<BigQueryEngine>): Int {
        if (sessionMode) {
            ensureSession()
        }
        return super.execute(delete)
    }

    override fun execute(loadData: LoadData) {
        if (loadData.fileSource.format != "CSV") {
            throw NotImplementedError("Loading data in '${loadData.fileSource.format}' format is not yet supported.")
        }
        if (loadData.columns == null) {
            throw NotImplementedError("Loading data with auto-detection of the schema is not yet supported.")
        }

        check(loadData.table !is TemporaryTable<*> || sessionMode) { "Loading data into a temporary table would require a session, but session mode is turned off." }
        if (sessionMode) {
            ensureSession()
        }

        if (loadData.overwrite) {
            if (topState.containsTable(loadData.table.qualifiedName)) {
                topState.removeTable(loadData.table)
            }
            topState.addTable(loadData.table)
        } else if (!topState.containsTable(loadData.table.qualifiedName)) {
            topState.addTable(loadData.table)
        }
        val (table, data) = topState.getTable(loadData.table.qualifiedName)

        val csvFormat = CSVFormat.DEFAULT.builder()
            .setHeader(*loadData.columns!!.map { it.name }.toTypedArray())
            .setLenientEof(true)
            .apply { loadData.fileSource.fieldDelimiter?.let { setDelimiter(it) } }
            .apply { loadData.fileSource.quote?.let { setQuote(it) } }
            .setNullString("")
            .get()
        loadData.fileSource.uris.forEach { uri ->
            FileReader(uri.getPath()).use { file ->
                val records = csvFormat.parse(file).stream()
                records
                    .skip(loadData.fileSource.skipLeadingRows?.toLong() ?: 0)
                    .map { record ->
                        DataRow(table.columns.associate { tableColumn ->
                            tableColumn.name to
                                    loadData.columns!!
                                        .find { column -> column.name == tableColumn.name }
                                        ?.let { dataColumn -> record[dataColumn.name] }
                                        .let { stringValue ->
                                            when (tableColumn.type.naturalKType()) {
                                                typeOf<Boolean>() -> stringValue?.toBoolean()
                                                typeOf<Long>() -> stringValue?.toLong()
                                                typeOf<BigDecimal>() -> stringValue?.toBigDecimal()
                                                typeOf<Instant>() -> stringValue?.let { Instant.parse(it) }
                                                typeOf<LocalDate>() -> stringValue?.let { LocalDate.parse(it) }
                                                typeOf<String>() -> stringValue
                                                else -> throw NotImplementedError("Loading data into a column of type '${tableColumn.type}' is not yet supported.")
                                            }
                                        }
                        })
                    }
                    .forEach { data.add(it) }
            }
        }
    }

    override fun execute(beginTransaction: BeginTransaction<BigQueryEngine>) {
        check(topState !is TransactionStateOverlay<BigQueryEngine>) { "There is already an open transaction and nested transactions are not supported." }
        check(sessionMode) { "Beginning a transaction would require a session, but session mode is turned off." }
            .also { ensureSession() }
        super.execute(beginTransaction)
    }

    override fun setSessionMode(sessionMode: Boolean) {
        if (!sessionMode && activeSession != null) {
            if (topState is TransactionStateOverlay<BigQueryEngine>) {
                execute(RollbackTransaction())
            }
            activeSession = null
        }
        this.sessionMode = sessionMode
    }

    private fun ensureSession() {
        activeSession = activeSession ?: SessionState(rootState).also { topState = it }
    }

    init {
        unregisterExpressionSimulator(rocks.frieler.kraftsql.expressions.Constant::class)
        registerExpressionSimulator(ConstantSimulator())
        unregisterExpressionSimulator(rocks.frieler.kraftsql.expressions.Row::class)
        registerExpressionSimulator(StructSimulator())
        registerExpressionSimulator(ReplaceSimulator())
        registerExpressionSimulator(TimestampSimulator())
        registerExpressionSimulator(JsonValueSimulator())
        registerExpressionSimulator(JsonValueArraySimulator())
    }
}
