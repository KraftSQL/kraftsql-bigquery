package rocks.frieler.kraftsql.bq.objects

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.objects.Data

typealias Data<T> = Data<BigQueryEngine, T>
