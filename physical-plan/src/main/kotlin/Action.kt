package io.andygrove.kquery.physical

import io.andygrove.kquery.datatypes.ShuffleId
import io.andygrove.kquery.logical.LogicalPlan

interface Action

data class QueryAction(val plan: LogicalPlan) : Action

data class ShuffleIdAction(val shuffleId: ShuffleId) : Action
