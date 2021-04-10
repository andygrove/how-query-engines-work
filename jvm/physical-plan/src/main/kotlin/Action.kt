package io.andygrove.physical

import io.andygrove.datatypes.ShuffleId
import io.andygrove.logical.LogicalPlan

interface Action

data class QueryAction(val plan: LogicalPlan) : Action

data class ShuffleIdAction(val shuffleId: ShuffleId) : Action
