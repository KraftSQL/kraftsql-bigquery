package rocks.frieler.kraftsql.bq.objects

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.objects.ConstantData

class ConstantData<T : Any> : ConstantData<BigQueryEngine, T> {
    constructor(items: Iterable<T>) : super(items)

    constructor(vararg items: T) : super(*items)
}
