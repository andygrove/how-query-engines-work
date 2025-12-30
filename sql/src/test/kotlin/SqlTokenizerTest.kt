// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package io.andygrove.kquery.sql

import kotlin.test.assertEquals
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.TestInstance

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SqlTokenizerTest {

  @Test
  fun `tokenize simple SELECT`() {
    val expected =
        listOf(
            Token("SELECT", Keyword.SELECT, 6),
            Token("id", Literal.IDENTIFIER, 9),
            Token(",", Symbol.COMMA, 10),
            Token("first_name", Literal.IDENTIFIER, 21),
            Token(",", Symbol.COMMA, 22),
            Token("last_name", Literal.IDENTIFIER, 32),
            Token("FROM", Keyword.FROM, 37),
            Token("employee", Literal.IDENTIFIER, 46))
    val actual =
        SqlTokenizer("SELECT id, first_name, last_name FROM employee")
            .tokenize()
            .tokens
    assertEquals(expected, actual)
  }

  @Test
  fun `projection with binary expression`() {
    val expected =
        listOf(
            Token("SELECT", Keyword.SELECT, 6),
            Token("salary", Literal.IDENTIFIER, 13),
            Token("*", Symbol.STAR, 15),
            Token("0.1", Literal.DOUBLE, 19),
            Token("FROM", Keyword.FROM, 24),
            Token("employee", Literal.IDENTIFIER, 33))
    val actual =
        SqlTokenizer("SELECT salary * 0.1 FROM employee").tokenize().tokens
    assertEquals(expected, actual)
  }

  @Test
  fun `projection with aliased binary expression`() {
    val expected =
        listOf(
            Token("SELECT", Keyword.SELECT, 6),
            Token("salary", Literal.IDENTIFIER, 13),
            Token("*", Symbol.STAR, 15),
            Token("0.1", Literal.DOUBLE, 19),
            Token("AS", Keyword.AS, 22),
            Token("bonus", Literal.IDENTIFIER, 28),
            Token("FROM", Keyword.FROM, 33),
            Token("employee", Literal.IDENTIFIER, 42))
    val actual =
        SqlTokenizer("SELECT salary * 0.1 AS bonus FROM employee")
            .tokenize()
            .tokens
    assertEquals(expected, actual)
  }

  @Test
  fun `tokenize SELECT with WHERE`() {
    val expected =
        listOf(
            Token("SELECT", Keyword.SELECT, 6),
            Token("a", Literal.IDENTIFIER, 8),
            Token(",", Symbol.COMMA, 9),
            Token("b", Literal.IDENTIFIER, 11),
            Token("FROM", Keyword.FROM, 16),
            Token("employee", Literal.IDENTIFIER, 25),
            Token("WHERE", Keyword.WHERE, 31),
            Token("state", Literal.IDENTIFIER, 37),
            Token("=", Symbol.EQ, 39),
            Token("CO", Literal.STRING, 44))
    val actual =
        SqlTokenizer("SELECT a, b FROM employee WHERE state = 'CO'")
            .tokenize()
            .tokens
    assertEquals(expected, actual)
  }

  @Test
  fun `tokenize SELECT with aggregates`() {
    val expected =
        listOf(
            Token("SELECT", Keyword.SELECT, 6),
            Token("state", Literal.IDENTIFIER, 12),
            Token(",", Symbol.COMMA, 13),
            Token("MAX", Keyword.MAX, 17),
            Token("(", Symbol.LEFT_PAREN, 18),
            Token("salary", Literal.IDENTIFIER, 24),
            Token(")", Symbol.RIGHT_PAREN, 25),
            Token("FROM", Keyword.FROM, 30),
            Token("employee", Literal.IDENTIFIER, 39),
            Token("GROUP", Keyword.GROUP, 45),
            Token("BY", Keyword.BY, 48),
            Token("state", Literal.IDENTIFIER, 54))
    val actual =
        SqlTokenizer("SELECT state, MAX(salary) FROM employee GROUP BY state")
            .tokenize()
            .tokens
    assertEquals(expected, actual)
  }

  @Test
  fun `tokenize SELECT with aggregates and HAVING`() {
    val expected =
        listOf(
            Token("SELECT", Keyword.SELECT, 6),
            Token("state", Literal.IDENTIFIER, 12),
            Token(",", Symbol.COMMA, 13),
            Token("MAX", Keyword.MAX, 17),
            Token("(", Symbol.LEFT_PAREN, 18),
            Token("salary", Literal.IDENTIFIER, 24),
            Token(")", Symbol.RIGHT_PAREN, 25),
            Token("FROM", Keyword.FROM, 30),
            Token("employee", Literal.IDENTIFIER, 39),
            Token("GROUP", Keyword.GROUP, 45),
            Token("BY", Keyword.BY, 48),
            Token("state", Literal.IDENTIFIER, 54),
            Token("HAVING", Keyword.HAVING, 61),
            Token("MAX", Keyword.MAX, 65),
            Token("(", Symbol.LEFT_PAREN, 66),
            Token("salary", Literal.IDENTIFIER, 72),
            Token(")", Symbol.RIGHT_PAREN, 73),
            Token(">", Symbol.GT, 75),
            Token("10", Literal.LONG, 78))
    val actual =
        SqlTokenizer(
                "SELECT state, MAX(salary) FROM employee GROUP BY state HAVING MAX(salary) > 10")
            .tokenize()
            .tokens
    assertEquals(expected, actual)
  }

  @Test
  fun `tokenize compound operators`() {
    val expected =
        listOf(
            Token("a", Literal.IDENTIFIER, 1),
            Token(">=", Symbol.GT_EQ, 4),
            Token("b", Literal.IDENTIFIER, 6),
            Token("OR", Keyword.OR, 9),
            Token("a", Literal.IDENTIFIER, 11),
            Token("<=", Symbol.LT_EQ, 14),
            Token("b", Literal.IDENTIFIER, 16),
            Token("OR", Keyword.OR, 19),
            Token("a", Literal.IDENTIFIER, 21),
            Token("<>", Symbol.LT_GT, 24),
            Token("b", Literal.IDENTIFIER, 26),
            Token("OR", Keyword.OR, 29),
            Token("a", Literal.IDENTIFIER, 31),
            Token("!=", Symbol.BANG_EQ, 34),
            Token("b", Literal.IDENTIFIER, 36))
    val actual =
        SqlTokenizer("a >= b OR a <= b OR a <> b OR a != b").tokenize().tokens
    assertEquals(expected, actual)
  }

  @Test
  fun `tokenize long values`() {
    val expected =
        listOf(
            Token("123456789", Literal.LONG, 9),
            Token("+", Symbol.PLUS, 11),
            Token("987654321", Literal.LONG, 21))
    val actual = SqlTokenizer("123456789 + 987654321").tokenize().tokens
    assertEquals(expected, actual)
  }

  @Test
  fun `tokenize float double values`() {
    val expected =
        listOf(
            Token("123456789.00", Literal.DOUBLE, 12),
            Token("+", Symbol.PLUS, 14),
            Token("987654321.001", Literal.DOUBLE, 28))
    val actual = SqlTokenizer("123456789.00 + 987654321.001").tokenize().tokens
    assertEquals(expected, actual)
  }

  @Test
  fun `tokenize table group`() {
    val expected =
        listOf(
            Token("select", Keyword.SELECT, 6),
            Token("*", Symbol.STAR, 8),
            Token("from", Keyword.FROM, 13),
            Token("group", Literal.IDENTIFIER, 19))
    val actual = SqlTokenizer("select * from group").tokenize().tokens
    assertEquals(expected, actual)
  }

  @Test
  fun `tokenize symbol after identifier`() {
    // This test exposes a bug in scanSymbol where 'offset' is used instead of 'startOffset'
    val expected =
        listOf(
            Token("a", Literal.IDENTIFIER, 1),
            Token("+", Symbol.PLUS, 2),
            Token("b", Literal.IDENTIFIER, 3))
    val actual = SqlTokenizer("a+b").tokenize().tokens
    assertEquals(expected, actual)
  }

  @Test
  fun `tokenize multiple symbols without spaces`() {
    // Another test for the scanSymbol bug - symbols in middle of expression
    val expected =
        listOf(
            Token("x", Literal.IDENTIFIER, 1),
            Token("*", Symbol.STAR, 2),
            Token("y", Literal.IDENTIFIER, 3),
            Token("+", Symbol.PLUS, 4),
            Token("z", Literal.IDENTIFIER, 5))
    val actual = SqlTokenizer("x*y+z").tokenize().tokens
    assertEquals(expected, actual)
  }

  @Test
  fun `tokenize ORDER as table name at end of query`() {
    // This test exposes the bounds check bug in processAmbiguousIdentifier
    // "ORDER" at end without "BY" following should be treated as identifier
    val expected =
        listOf(
            Token("SELECT", Keyword.SELECT, 6),
            Token("*", Symbol.STAR, 8),
            Token("FROM", Keyword.FROM, 13),
            Token("order", Literal.IDENTIFIER, 19))
    val actual = SqlTokenizer("SELECT * FROM order").tokenize().tokens
    assertEquals(expected, actual)
  }

  @Test
  fun `tokenize ORDER as table name with trailing space`() {
    // Edge case: "ORDER " with trailing space but nothing after
    val expected =
        listOf(
            Token("SELECT", Keyword.SELECT, 6),
            Token("*", Symbol.STAR, 8),
            Token("FROM", Keyword.FROM, 13),
            Token("order", Literal.IDENTIFIER, 19))
    val actual = SqlTokenizer("SELECT * FROM order ").tokenize().tokens
    assertEquals(expected, actual)
  }

  @Test
  fun `tokenize backtick identifier`() {
    // This test exposes the bug where backtick identifier uses 'offset' instead of 'startOffset +
    // 1'
    val expected =
        listOf(
            Token("SELECT", Keyword.SELECT, 6),
            Token("my column", Literal.IDENTIFIER, 18),
            Token("FROM", Keyword.FROM, 23),
            Token("t", Literal.IDENTIFIER, 25))
    val actual = SqlTokenizer("SELECT `my column` FROM t").tokenize().tokens
    assertEquals(expected, actual)
  }

  @Test
  fun `tokenize escaped single quotes in string`() {
    // SQL standard: '' inside a string represents a single quote
    val expected = listOf(Token("it's", Literal.STRING, 7))
    val actual = SqlTokenizer("'it''s'").tokenize().tokens
    assertEquals(expected, actual)
  }

  @Test
  fun `tokenize number starting with dot`() {
    // Numbers like .5 should be valid
    val expected = listOf(Token(".5", Literal.DOUBLE, 2))
    val actual = SqlTokenizer(".5").tokenize().tokens
    assertEquals(expected, actual)
  }

  @Test
  fun `tokenize unrecognized character should fail`() {
    // Unrecognized characters like $ should produce an error, not silently stop
    val exception =
        org.junit.jupiter.api.assertThrows<TokenizeException> {
          SqlTokenizer("SELECT $ FROM t").tokenize()
        }
    // Should contain info about the bad character
    assert(exception.msg.contains("$") || exception.msg.isNotEmpty())
  }

  @Test
  fun `tokenize date literal`() {
    val expected =
        listOf(
            Token("date", Keyword.DATE, 4),
            Token("1998-12-01", Literal.STRING, 17))
    val actual = SqlTokenizer("date '1998-12-01'").tokenize().tokens
    assertEquals(expected, actual)
  }

  @Test
  fun `tokenize interval literal`() {
    val expected =
        listOf(
            Token("interval", Keyword.INTERVAL, 8),
            Token("68 days", Literal.STRING, 18))
    val actual = SqlTokenizer("interval '68 days'").tokenize().tokens
    assertEquals(expected, actual)
  }
}
