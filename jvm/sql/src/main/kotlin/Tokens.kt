package org.ballistacompute.sql

interface Token

data class IdentifierToken(val text: String) : Token {
    override fun toString(): String {
        return text
    }
}

abstract class TokenBase(val text: String) : Token {
    override fun toString(): String {
        return text
    }

    override fun hashCode(): Int {
        return this.toString().hashCode()
    }

    override fun equals(other: Any?): Boolean {
        return this.toString() == other.toString()
    }
}

class LiteralStringToken(text: String) : TokenBase(text)
class LiteralLongToken(text: String) : TokenBase(text)
class LiteralDoubleToken(text: String) : TokenBase(text)
class KeywordToken(text: String) : TokenBase(text)
class OperatorToken(text: String) : TokenBase(text)

class CommaToken() : TokenBase(",")
class LParenToken() : TokenBase("(")
class RParenToken() : TokenBase(")")