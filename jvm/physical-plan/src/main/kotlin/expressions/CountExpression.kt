// Copyright 2020 Andy Grove
//
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

package io.andygrove.kquery.physical.expressions

class CountExpression(private val expr: Expression) : AggregateExpression {

  override fun inputExpression(): Expression {
    return expr
  }

  override fun createAccumulator(): Accumulator {
    return CountAccumulator()
  }

  override fun toString(): String {
    return "COUNT($expr)"
  }
}

class CountAccumulator : Accumulator {

  var count: Int = 0

  override fun accumulate(value: Any?) {
    if (value != null) {
      count++
    }
  }

  override fun finalValue(): Any? {
    return count
  }
}
