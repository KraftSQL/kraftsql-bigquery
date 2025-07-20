package rocks.frieler.kraftsql.bq.engine

import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigquery.QueryJobConfiguration
import rocks.frieler.kraftsql.ddl.CreateTable
import rocks.frieler.kraftsql.dml.InsertInto
import rocks.frieler.kraftsql.engine.Connection
import rocks.frieler.kraftsql.engine.DefaultConnection
import rocks.frieler.kraftsql.objects.Row
import rocks.frieler.kraftsql.queries.Select
import kotlin.reflect.KClass
import kotlin.reflect.full.starProjectedType

class BigQueryConnection(
    private val bigquery: BigQuery,
) : Connection<BigQueryEngine> {

    override fun <T : Any> execute(select: Select<BigQueryEngine, T>, type: KClass<T>): List<T> {
        val result = bigquery.query(QueryJobConfiguration.newBuilder(select.sql()).setUseLegacySql(false).build())

        return result.iterateAll().map { row ->
            if (type != Row::class) {
                val constructor = type.constructors.first()
                constructor.callBy(constructor.parameters.associateWith { param ->
                    when (param.type) {
                        Integer::class.starProjectedType -> row.get(param.name).numericValue.toInt()
                        Long::class.starProjectedType -> row.get(param.name).numericValue.toLong()
                        String::class.starProjectedType -> row.get(param.name).stringValue
                        else -> throw NotImplementedError("Unsupported field type ${param.type}")
                    }
                })
            } else {
                @Suppress("UNCHECKED_CAST")
                Row(result.schema!!.fields.associate { field -> field.name to row.get(field.name).value }) as T
            }
        }
    }

    override fun execute(createTable: CreateTable<BigQueryEngine>) {
        TODO("Not yet implemented")
    }

    override fun execute(insertInto: InsertInto<BigQueryEngine, *>): Int {
        TODO("Not yet implemented")
    }

    object Default : DefaultConnection<BigQueryEngine>() {
        override fun instantiate(): Connection<BigQueryEngine> {
            return BigQueryConnection(BigQueryOptions.getDefaultInstance().service)
        }
    }
}
