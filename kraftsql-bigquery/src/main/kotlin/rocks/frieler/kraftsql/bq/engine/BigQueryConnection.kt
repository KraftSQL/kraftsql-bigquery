package rocks.frieler.kraftsql.bq.engine

import rocks.frieler.kraftsql.bq.dml.LoadData
import rocks.frieler.kraftsql.engine.Connection

interface BigQueryConnection : Connection<BigQueryEngine> {
    fun execute(loadData: LoadData)

    fun setSessionMode(sessionMode: Boolean)
}
