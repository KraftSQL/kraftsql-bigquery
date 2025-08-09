package rocks.frieler.kraftsql.bq.objects

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.engine.BigQueryORMapping
import rocks.frieler.kraftsql.objects.ConstantData

class ConstantData<T : Any> : ConstantData<BigQueryEngine, T> {
    constructor(items: Iterable<T>) : super(BigQueryORMapping, items)

    constructor(vararg items: T) : super(BigQueryORMapping, *items)
}
