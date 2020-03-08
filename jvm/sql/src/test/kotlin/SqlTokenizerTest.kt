package org.ballistacompute.sql

import org.junit.Test
import org.junit.jupiter.api.TestInstance
import kotlin.test.assertEquals

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SqlTokenizerTest {

    @Test
    fun `tokenize simple SELECT`() {
        val expected = listOf(
                KeywordToken("SELECT"),
                IdentifierToken("a"),
                CommaToken(),
                IdentifierToken("b"),
                KeywordToken("FROM"),
                IdentifierToken("employee")
        )
        val actual = tokenize("SELECT a, b FROM employee")
        assertEquals(expected, actual)
    }

    @Test
    fun `tokenize SELECT with WHERE`() {
        val expected = listOf(
                KeywordToken("SELECT"),
                IdentifierToken("a"),
                CommaToken(),
                IdentifierToken("b"),
                KeywordToken("FROM"),
                IdentifierToken("employee"),
                KeywordToken("WHERE"),
                IdentifierToken("state"),
                OperatorToken("="),
                LiteralStringToken("CO")
        )
        val actual = tokenize("SELECT a, b FROM employee WHERE state = 'CO'")
        assertEquals(expected, actual)
    }

    private fun tokenize(sql: String) : List<Token> {
        return SqlTokenizer(sql).tokenize().tokens
    }
}