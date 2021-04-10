package io.andygrove.queryengine.physical

import io.andygrove.queryengine.datatypes.ShuffleId
import io.andygrove.queryengine.logical.LogicalPlan

interface Action

data class QueryAction(val plan: LogicalPlan) : Action

data class ShuffleIdAction(val shuffleId: ShuffleId) : Action
