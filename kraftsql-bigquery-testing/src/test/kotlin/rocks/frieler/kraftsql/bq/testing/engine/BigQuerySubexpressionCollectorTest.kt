package rocks.frieler.kraftsql.bq.testing.engine

import io.kotest.matchers.collections.shouldContainExactlyInAnyOrder
import org.junit.jupiter.api.Test
import org.mockito.Mockito.mock
import rocks.frieler.kraftsql.bq.engine.BigQueryEngine
import rocks.frieler.kraftsql.bq.expressions.ArrayLength
import rocks.frieler.kraftsql.bq.expressions.JsonValue
import rocks.frieler.kraftsql.bq.expressions.JsonValueArray
import rocks.frieler.kraftsql.bq.expressions.Replace
import rocks.frieler.kraftsql.bq.expressions.Timestamp
import rocks.frieler.kraftsql.bq.expressions.Unnest
import rocks.frieler.kraftsql.expressions.Expression

class BigQuerySubexpressionCollectorTest {
    private val subexpressionCollector = BigQuerySubexpressionCollector()

    @Test
    fun `GenericSubexpressionCollector can collect array expression of ArrayLength`() {
        val arrayExpression = org.mockito.kotlin.mock<Expression<BigQueryEngine, Array<*>>>()
        val arrayLength = ArrayLength(arrayExpression)

        val subexpressions = subexpressionCollector.getSubexpressions(arrayLength)

        subexpressions shouldContainExactlyInAnyOrder listOf(arrayLength.array)
    }

    @Test
    fun `BigQuerySubexpressionCollector collects JSON string and path of JsonValue`() {
        val jsonValue = JsonValue(mock(), mock())

        val subexpressions = subexpressionCollector.getSubexpressions(jsonValue)

        subexpressions shouldContainExactlyInAnyOrder listOf(jsonValue.jsonString, jsonValue.jsonPath)
    }

    @Test
    fun `BigQuerySubexpressionCollector collects only JSON string of JsonValue without path`() {
        val jsonValue = JsonValue(mock())

        val subexpressions = subexpressionCollector.getSubexpressions(jsonValue)

        subexpressions shouldContainExactlyInAnyOrder listOf(jsonValue.jsonString)
    }

    @Test
    fun `BigQuerySubexpressionCollector collects JSON string and path of JsonValueArray`() {
        val jsonValueArray = JsonValueArray(mock(), mock())

        val subexpressions = subexpressionCollector.getSubexpressions(jsonValueArray)

        subexpressions shouldContainExactlyInAnyOrder listOf(jsonValueArray.jsonString, jsonValueArray.jsonPath)
    }

    @Test
    fun `BigQuerySubexpressionCollector collects only JSON string of JsonValueArray without path`() {
        val jsonValueArray = JsonValueArray(mock())

        val subexpressions = subexpressionCollector.getSubexpressions(jsonValueArray)

        subexpressions shouldContainExactlyInAnyOrder listOf(jsonValueArray.jsonString)
    }

    @Test
    fun `BigQuerySubexpressionCollector collects original value, from- and to-pattern of Replace`() {
        val replace = Replace(mock(), mock(), mock())

        val subexpressions = subexpressionCollector.getSubexpressions(replace)

        subexpressions shouldContainExactlyInAnyOrder listOf(replace.originalValue, replace.fromPattern, replace.toPattern)
    }

    @Test
    fun `BigQuerySubexpressionCollector collects string expression of Timestamp`() {
        val timestamp = Timestamp(mock())

        val subexpressions = subexpressionCollector.getSubexpressions(timestamp)

        subexpressions shouldContainExactlyInAnyOrder listOf(timestamp.stringExpression)
    }

    @Test
    fun `BigQuerySubexpressionCollector collects array expression of Unnest`() {
        val unnest = Unnest(mock<Expression<BigQueryEngine, Array<Any>>>())

        val subexpressions = subexpressionCollector.getSubexpressions(unnest)

        subexpressions shouldContainExactlyInAnyOrder listOf(unnest.arrayExpression)
    }
}
