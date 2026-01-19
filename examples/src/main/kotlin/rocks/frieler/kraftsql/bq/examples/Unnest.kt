package rocks.frieler.kraftsql.bq.examples

import rocks.frieler.kraftsql.bq.dql.Select
import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.engine.Types
import rocks.frieler.kraftsql.bq.examples.data.Product
import rocks.frieler.kraftsql.bq.examples.data.products
import rocks.frieler.kraftsql.bq.examples.data.withSampleData
import rocks.frieler.kraftsql.bq.expressions.Constant
import rocks.frieler.kraftsql.bq.expressions.Struct
import rocks.frieler.kraftsql.bq.expressions.Unnest
import rocks.frieler.kraftsql.bq.objects.collect
import rocks.frieler.kraftsql.dql.CrossJoin
import rocks.frieler.kraftsql.dql.DataExpressionData
import rocks.frieler.kraftsql.dql.Projection
import rocks.frieler.kraftsql.dql.QuerySource
import rocks.frieler.kraftsql.expressions.Array
import rocks.frieler.kraftsql.expressions.Cast
import rocks.frieler.kraftsql.expressions.Column
import rocks.frieler.kraftsql.objects.DataRow

fun main() {
    Select<DataRow>(
        source = QuerySource(DataExpressionData(Unnest(
            Array(
                Struct<DataRow>(mapOf("x" to Constant(1), "y" to Constant("foo"))),
                Struct(mapOf("x" to Constant(3), "y" to Constant("bar"))),
            ))),
            "struct_value"),
        columns = listOf(
            Projection(Column<BigQueryEngine, Int>("x").withQualifier("struct_value")),
            Projection(Column<BigQueryEngine, String>("y").withQualifier("struct_value")),
            //Projection(Column<BigQueryEngine, DataRow>("struct_value")),
        ).takeIf { false }
    )
        .also { println(it.sql()) }
        .also { println(it.columnNames) }
        .collect()
        .forEach { println(it) }

    withSampleData {
        Select<DataRow>(
            source = QuerySource(products),
            joins = listOf(
                CrossJoin(QuerySource(DataExpressionData(Unnest(products[Product::tags])), "tag"))
            )
        )
            .also { println(it.sql()) }
            .collect()
            .forEach { println(it) }
    }
}
