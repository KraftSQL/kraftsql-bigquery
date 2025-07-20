package rocks.frieler.kraftsql.bq.objects

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.objects.Column
import rocks.frieler.kraftsql.objects.Table
import kotlin.reflect.KClass

class Table<T : Any> : Table<BigQueryEngine, T> {
    val datasetId: String

    constructor(datasetId: String, tableId: String, columns: List<Column<BigQueryEngine>>) : super(tableId, columns) {
        this.datasetId = datasetId
    }

    constructor(datasetId: String, tableId: String, type: KClass<T>) : super(BigQueryEngine, tableId, type) {
        this.datasetId = datasetId
    }

    override fun sql(): String {
        return "`${datasetId}`.${super.sql()}"
    }
}
