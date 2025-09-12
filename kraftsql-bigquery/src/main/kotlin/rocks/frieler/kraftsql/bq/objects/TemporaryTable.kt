package rocks.frieler.kraftsql.bq.objects

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.objects.Column
import kotlin.reflect.KClass

class TemporaryTable<T : Any> : Table<T> {
    constructor(name: String, columns: List<Column<BigQueryEngine>>) :
            super("", "", name, columns)

    constructor(name: String, type: KClass<T>) :
            super("", "", name, type)

    override fun sql() = "`$name`"
}
