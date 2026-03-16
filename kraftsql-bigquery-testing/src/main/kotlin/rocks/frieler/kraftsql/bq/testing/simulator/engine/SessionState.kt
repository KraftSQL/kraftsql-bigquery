package rocks.frieler.kraftsql.bq.testing.simulator.engine

import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.objects.TemporaryTable
import rocks.frieler.kraftsql.objects.DataRow
import rocks.frieler.kraftsql.objects.Table
import rocks.frieler.kraftsql.testing.simulator.engine.EngineState

class SessionState(
    private val parent: EngineState<BigQueryEngine>,
) : EngineState<BigQueryEngine>() {
    override fun containsTable(name: String): Boolean {
        return super.containsTable(name) || parent.containsTable(name)
    }

    override fun findTable(name: String): Pair<Table<BigQueryEngine, *>, MutableList<DataRow>>? {
        return super.findTable(name) ?: parent.findTable(name)
    }

    override fun addTable(table: Table<BigQueryEngine, *>) {
        if (table is TemporaryTable<*>) {
            super.addTable(table)
        } else {
            parent.addTable(table)
        }
    }

    override fun removeTable(table: Table<BigQueryEngine, *>) {
        if (table is TemporaryTable<*>) {
            super.removeTable(table)
        } else {
            parent.removeTable(table)
        }
    }

    override fun writeTable(table: Table<BigQueryEngine, *>, data: List<DataRow>) {
        if (table is TemporaryTable<*>) {
            super.writeTable(table, data)
        } else {
            parent.writeTable(table, data)
        }
    }
}
