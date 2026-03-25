package rocks.frieler.kraftsql.bq.testing.simulator.engine

import org.apache.commons.csv.CSVFormat
import rocks.frieler.kraftsql.bq.dml.LoadData
import rocks.frieler.kraftsql.bq.engine.BigQueryConnection
import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.objects.TemporaryTable
import rocks.frieler.kraftsql.bq.testing.simulator.expressions.BigQueryExpressionEvaluator
import rocks.frieler.kraftsql.ddl.CreateTable
import rocks.frieler.kraftsql.ddl.DropTable
import rocks.frieler.kraftsql.dml.BeginTransaction
import rocks.frieler.kraftsql.dml.Delete
import rocks.frieler.kraftsql.dml.InsertInto
import rocks.frieler.kraftsql.dml.RollbackTransaction
import rocks.frieler.kraftsql.dql.Select
import rocks.frieler.kraftsql.engine.Connection
import rocks.frieler.kraftsql.objects.DataRow
import rocks.frieler.kraftsql.testing.simulator.engine.EngineState
import rocks.frieler.kraftsql.testing.simulator.engine.GenericEngineSimulator
import rocks.frieler.kraftsql.testing.simulator.engine.GenericQueryEvaluator
import rocks.frieler.kraftsql.testing.simulator.engine.SimulatorORMapping
import rocks.frieler.kraftsql.testing.simulator.engine.TransactionStateOverlay
import rocks.frieler.kraftsql.testing.simulator.expressions.GenericExpressionEvaluator
import java.io.FileReader
import java.math.BigDecimal
import java.time.Instant
import java.time.LocalDate
import java.util.WeakHashMap
import kotlin.reflect.KClass
import kotlin.reflect.typeOf

/**
 * [rocks.frieler.kraftsql.testing.simulator.engine.EngineSimulator] for the [BigQueryEngine].
 *
 * @param orm the [SimulatorORMapping] to use for the engine, defaults to the [BigQuerySimulatorORMapping]
 * @param persistentState the [EngineState] to use for the engine, defaults to an [EngineState]
 * @param expressionEvaluator the [GenericExpressionEvaluator] to use for the engine, defaults to the [BigQueryExpressionEvaluator]
 * @param queryEvaluator the [GenericQueryEvaluator] to use for the engine, defaults to the [BigQueryQueryEvaluator]
 */
class BigQueryEngineSimulator(
    orm: SimulatorORMapping<BigQueryEngine> = BigQuerySimulatorORMapping,
    persistentState: EngineState<BigQueryEngine> = EngineState(),
    expressionEvaluator: GenericExpressionEvaluator<BigQueryEngine> = BigQueryExpressionEvaluator,
    queryEvaluator: GenericQueryEvaluator<BigQueryEngine> = BigQueryQueryEvaluator,
) : GenericEngineSimulator<BigQueryEngine>(orm, persistentState, expressionEvaluator, queryEvaluator) {
    context(connection: Connection<BigQueryEngine>)
    override fun <T : Any> execute(select: Select<BigQueryEngine, T>, type: KClass<T>): List<T> {
        if (connection is BigQueryConnection && connection.sessionMode) {
            ensureSession(connection)
        }
        return super.execute(select, type)
    }

    context(connection: Connection<BigQueryEngine>)
    override fun execute(createTable: CreateTable<BigQueryEngine>) {
        if (createTable.table is TemporaryTable<*>) {
            throw UnsupportedOperationException("The BigQuery API does not support creation of temporary tables.")
        }
        super.execute(createTable)
    }

    context(connection: Connection<BigQueryEngine>)
    override fun execute(dropTable: DropTable<BigQueryEngine>) {
        if (dropTable.table is TemporaryTable<*>) {
            throw UnsupportedOperationException("The BigQuery API does not support dropping temporary tables.")
        }
        super.execute(dropTable)
    }

    context(connection: Connection<BigQueryEngine>)
    override fun execute(insertInto: InsertInto<BigQueryEngine, *>): Int {
        if (connection is BigQueryConnection && connection.sessionMode) {
            ensureSession(connection)
        }
        return super.execute(insertInto)
    }

    context(connection: Connection<BigQueryEngine>)
    override fun execute(delete: Delete<BigQueryEngine>): Int {
        if (connection is BigQueryConnection && connection.sessionMode) {
            ensureSession(connection)
        }
        return super.execute(delete)
    }

    context(connection: Connection<BigQueryEngine>)
    fun execute(loadData: LoadData) {
        if (loadData.fileSource.format != "CSV") {
            throw NotImplementedError("Loading data in '${loadData.fileSource.format}' format is not yet supported.")
        }
        if (loadData.columns == null) {
            throw NotImplementedError("Loading data with auto-detection of the schema is not yet supported.")
        }

        check(loadData.table !is TemporaryTable<*> || (connection is BigQueryConnection && connection.sessionMode)) { "Loading data into a temporary table would require a session, but session mode is turned off." }
        if (connection is BigQueryConnection && connection.sessionMode) {
            ensureSession(connection)
        }

        if (loadData.overwrite) {
            if (getTopState(connection).containsTable(loadData.table.qualifiedName)) {
                getTopState(connection).removeTable(loadData.table)
            }
            getTopState(connection).addTable(loadData.table)
        } else if (!getTopState(connection).containsTable(loadData.table.qualifiedName)) {
            getTopState(connection).addTable(loadData.table)
        }
        val (table, data) = getTopState(connection).getTable(loadData.table.qualifiedName)

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
                        DataRow(table.columns.map { tableColumn ->
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

    context(connection: Connection<BigQueryEngine>)
    override fun execute(beginTransaction: BeginTransaction<BigQueryEngine>) {
        check(getTopState(connection) !is TransactionStateOverlay<BigQueryEngine>) { "There is already an open transaction and nested transactions are not supported." }
        check(connection is BigQueryConnection && connection.sessionMode) { "Beginning a transaction would require a session, but session mode is turned off." }
            .also { ensureSession(connection) }
        super.execute(beginTransaction)
    }

    private val sessions = WeakHashMap<Connection<BigQueryEngine>, SessionState>()

    private fun ensureSession(connection: Connection<BigQueryEngine>) {
        sessions.computeIfAbsent(connection) { SessionState(getTopState(connection)).also { connectionStateOverlays[connection] = it } }
    }

    context(connection: Connection<BigQueryEngine>)
    fun abortSession() {
        if (getTopState(connection) is TransactionStateOverlay<BigQueryEngine>) {
            execute(RollbackTransaction())
        }
        sessions.remove(connection)?.let { connectionStateOverlays.remove(connection) }
    }
}
