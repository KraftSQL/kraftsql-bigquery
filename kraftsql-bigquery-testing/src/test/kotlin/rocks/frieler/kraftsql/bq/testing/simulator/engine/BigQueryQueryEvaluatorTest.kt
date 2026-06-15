package rocks.frieler.kraftsql.bq.testing.simulator.engine

import io.kotest.assertions.throwables.shouldThrow
import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import rocks.frieler.kraftsql.bq.dql.Select
import rocks.frieler.kraftsql.bq.dsl.Select
import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.expressions.Constant
import rocks.frieler.kraftsql.bq.objects.ConstantData
import rocks.frieler.kraftsql.bq.testing.simulator.expressions.BigQuerySubexpressionCollector
import rocks.frieler.kraftsql.bq.testing.simulator.expressions.ConstantSimulator
import rocks.frieler.kraftsql.bq.testing.simulator.expressions.StructSimulator
import rocks.frieler.kraftsql.dql.LeftJoin
import rocks.frieler.kraftsql.dql.Projection
import rocks.frieler.kraftsql.dql.QuerySource
import rocks.frieler.kraftsql.dql.QuerySource.Companion.Alias
import rocks.frieler.kraftsql.dsl.`as`
import rocks.frieler.kraftsql.expressions.Column
import rocks.frieler.kraftsql.expressions.Min
import rocks.frieler.kraftsql.objects.DataRow
import rocks.frieler.kraftsql.testing.simulator.engine.EngineState
import rocks.frieler.kraftsql.testing.simulator.expressions.ColumnSimulator
import rocks.frieler.kraftsql.testing.simulator.expressions.GenericExpressionEvaluator
import java.sql.SQLException

class BigQueryQueryEvaluatorTest {
    private val queryEvaluator = BigQueryQueryEvaluator(
        BigQuerySimulatorORMapping,
        BigQuerySubexpressionCollector(),
        GenericExpressionEvaluator<BigQueryEngine>().apply {
            // register some essential ExpressionSimulators necessary for this test:
            registerExpressionSimulator(ConstantSimulator<Any>())
            registerExpressionSimulator(ColumnSimulator<BigQueryEngine, Any>())
            registerExpressionSimulator(StructSimulator())
        },
    )

    private val activeState = mock<EngineState<BigQueryEngine>>()

    @Test
    fun `BigQueryQueryEvaluator can simulate SELECT from constant data`() {
        val result = context(activeState) {
            queryEvaluator.selectRows(
                Select<DataRow>(
                    source = QuerySource(ConstantData(DataRow("name" to "foo"))),
                    columns = listOf(Projection(Column<BigQueryEngine, String>("name"))),
                )
            )
        }

        result shouldContainExactly listOf(DataRow("name" to "foo"))
    }

    @Test
    fun `BigQueryQueryEvaluator keeps explicit column alias`() {
        val result = Select<DataRow>(
            source = QuerySource(ConstantData(DataRow())),
            columns = listOf(Projection(Constant(42), "fortytwo")),
        ).let { context(activeState) { queryEvaluator.selectRows(it) } }

        result shouldContainExactly listOf(DataRow("fortytwo" to 42))
    }

    @Test
    fun `BigQueryQueryEvaluator names unaliased Column expression by the referenced Column`() {
        val result = Select<DataRow>(
            source = QuerySource(ConstantData(DataRow("fortytwo" to 42))),
            columns = listOf(Projection(Column<BigQueryEngine, Int>("fortytwo"))),
        ).let { context(activeState) { queryEvaluator.selectRows(it) } }

        result shouldContainExactly listOf(DataRow("fortytwo" to 42))
    }

    @Test
    fun `BigQueryQueryEvaluator names unaliased Column expression by the unqualified referenced Column`() {
        val result = Select<DataRow>(
            source = QuerySource(ConstantData(DataRow("fortytwo" to 42)), Alias("data")),
            columns = listOf(Projection(Column<BigQueryEngine, Int>(listOf("data"), "fortytwo"))),
        ).let { context(activeState) { queryEvaluator.selectRows(it) } }

        result shouldContainExactly listOf(DataRow("fortytwo" to 42))
    }

    @Test
    fun `BigQueryQueryEvaluator generates f{n}_ names for unaliased expressions`() {
        val result = Select<DataRow>(
            source = QuerySource(ConstantData(DataRow())),
            columns = listOf(Projection(Constant(42)), Projection(Constant(43))),
        ).let { context(activeState) { queryEvaluator.selectRows(it) } }

        result shouldContainExactly listOf(DataRow("f0_" to 42, "f1_" to 43))
    }

    @Test
    fun `BigQueryQueryEvaluator respects explicit f{n}_ alias when generating column names`() {
        val result = Select<DataRow>(
            source = QuerySource(ConstantData(DataRow())),
            columns = listOf(Projection(Constant(42), "f1_"), Projection(Constant(43))),
        ).let { context(activeState) { queryEvaluator.selectRows(it) } }

        result shouldContainExactly listOf(DataRow("f1_" to 42, "f0_" to 43))
    }

    @Test
    fun `BigQueryQueryEvaluator has correlated JOINs enabled`() {
        val leftSide = QuerySource(ConstantData(DataRow("id" to 1)))
        val rightSide = QuerySource(
            Select(
                source = QuerySource(ConstantData(DataRow())),
                columns = listOf(Projection(leftSide["id"], ""))
            ), Alias("id_from_left")
        )

        val result = Select<DataRow>(
            source = leftSide,
            joins = listOf(LeftJoin(rightSide, Constant(true))),
        ).let { context(activeState) { queryEvaluator.selectRows(it) } }

        result shouldContainExactlyInAnyOrder listOf(DataRow("id" to 1, "id_from_left" to 1))
    }

    @Test
    fun `BigQueryQueryEvaluator rejects grouping by Constant`() {
        val select = Select<DataRow> {
            from(ConstantData(DataRow()))
            groupBy(Constant("min"))
            column(Min(Constant(42)) `as` "min")
        }

        shouldThrow<SQLException> {
            context(activeState) { queryEvaluator.selectRows(select) }
        }
    }
}
