package rocks.frieler.kraftsql.bq.dml

import rocks.frieler.kraftsql.bq.engine.BigQueryConnection
import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.dml.BeginTransaction


fun BeginTransaction<BigQueryEngine>.execute() {
    BigQueryConnection.Default.get().execute(this)
}
