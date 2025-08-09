package rocks.frieler.kraftsql.bq.objects

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.engine.BigQueryORMapping
import rocks.frieler.kraftsql.objects.Column
import rocks.frieler.kraftsql.objects.Table
import kotlin.reflect.KClass

class Table<T : Any> : Table<BigQueryEngine, T> {
    val project: String?
        get() = database

    val dataset: String
        get() = schema!!

    constructor(project: String? = null, dataset: String, name: String, columns: List<Column<BigQueryEngine>>) :
            super(project, dataset, name, columns)

    constructor(project: String? = null, dataset: String, name: String, type: KClass<T>) :
            super(BigQueryORMapping, project, dataset, name, type)
}
