package rocks.frieler.kraftsql.bq.engine

import rocks.frieler.kraftsql.engine.Connection

interface BigQueryConnection : Connection<BigQueryEngine> {
    fun setSessionMode(sessionMode: Boolean)
}
