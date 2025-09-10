package rocks.frieler.kraftsql.bq.dml

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.dml.CommitTransaction

fun CommitTransaction<BigQueryEngine>.execute() {
    BigQueryEngine.DefaultConnection.get().execute(this)
}
