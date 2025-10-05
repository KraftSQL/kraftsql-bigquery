package rocks.frieler.kraftsql.bq.expressions

import io.kotest.matchers.shouldBe
import org.junit.jupiter.api.Test
import rocks.frieler.kraftsql.bq.engine.Types
import java.math.BigDecimal

class ConstantTest {
    @Test
    fun `constant BIGNUMERIC is written with type as prefix and value between quotes in SQL`() {
        val constantBigNumeric = Constant(BigDecimal("1.23"))

        constantBigNumeric.sql() shouldBe "${Types.BIGNUMERIC.sql()} '1.23'"
    }
}
