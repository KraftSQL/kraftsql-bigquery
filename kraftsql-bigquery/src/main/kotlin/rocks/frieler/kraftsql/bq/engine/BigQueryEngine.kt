package rocks.frieler.kraftsql.bq.engine

import rocks.frieler.kraftsql.engine.Engine
import rocks.frieler.kraftsql.engine.Type
import kotlin.reflect.KType

object BigQueryEngine : Engine<BigQueryEngine> {
    override fun getTypeFor(type: KType): Type {
        TODO("Not yet implemented")
    }
}
