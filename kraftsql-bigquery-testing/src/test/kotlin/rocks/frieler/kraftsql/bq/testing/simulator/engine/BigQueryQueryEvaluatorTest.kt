package rocks.frieler.kraftsql.bq.testing.simulator.engine

import io.kotest.matchers.collections.shouldContainExactly
import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import org.junit.jupiter.api.Test
import org.mockito.kotlin.mock
import rocks.frieler.kraftsql.bq.dql.Select
import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.expressions.Constant
import rocks.frieler.kraftsql.bq.objects.ConstantData
import rocks.frieler.kraftsql.dql.LeftJoin
import rocks.frieler.kraftsql.dql.Projection
import rocks.frieler.kraftsql.dql.QuerySource
import rocks.frieler.kraftsql.expressions.Column
import rocks.frieler.kraftsql.objects.DataRow
import rocks.frieler.kraftsql.testing.simulator.engine.EngineState

class BigQueryQueryEvaluatorTest {
    private val activeState = mock<EngineState<BigQueryEngine>>()

    @Test
    fun `BigQueryQueryEvaluator can simulate SELECT from constant data`() {
        val result = context(activeState) {
            BigQueryQueryEvaluator.selectRows(
                Select<DataRow>(
                    source = QuerySource(ConstantData(DataRow("name" to "foo"))),
                    columns = listOf(Projection(Column<BigQueryEngine, String>("name"))),
                )
            )
        }

        result shouldContainExactly listOf(DataRow("name" to "foo"))
    }

    @Test
    fun `BigQueryQueryEvaluator has correlated JOINs enabled`() {
        val leftSide = QuerySource(ConstantData(DataRow("id" to 1)))
        val rightSide = QuerySource(
            Select(
                source = QuerySource(ConstantData(DataRow())),
                columns = listOf(Projection(leftSide["id"]))
            ), "left"
        )

        val result = context(activeState) {
            BigQueryQueryEvaluator.selectRows(
                Select<DataRow>(
                    source = leftSide,
                    joins = listOf(LeftJoin(rightSide, Constant(true))),
                )
            )
        }

        result shouldContainExactlyInAnyOrder listOf(DataRow("id" to 1, "left.id" to 1))
    }
}
