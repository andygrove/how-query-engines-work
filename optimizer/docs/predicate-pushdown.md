# Predicate push down optimization rule

The idea is about moving filter as low in the tree as possible, so that rows that will not impact final result will be removed as early as possible. Correctness is a goal - any rewrite after optimization needs to provide absolutely same output data if optimization rule is turned off.
The ideal option also fuses together predicate with scan, but our code doesn't support it, so pushing filter as low as possible to sit right on top of Scan is good enough.
Main decisions for all this process is to find node types that block further push and stop there, placing Selection node right on top of them.
In this first version we also decided not to combine / split Selection(s), and not handle cases with column renamings / modification in projection. Selection becomes a blocker even if we could've moved our selection under consideration further.
From the get-go, our obvious blockers are: Scan (always lowest node) and Limit (as it will change computation results).
Aggregate can't be a blocker if Selection operates on grouping column (e.g. group by state). Selection would eliminate whole group before computation so it's safe.
Projection isn't a blocker if required column exists (cautious about same-name-different-col case).
Join in a complicated bit and needs to be handled carefully. If it's inner join - we have to check which subtree has related columns and pushdown filter there. If condition uses both left and right - we have to stop.
In case of left join we can only push down to the left, in case of right join - to the right. That's "preserved" part of join table. Null-supplying has to come in full always - as removing null-supplying doesn't reduce output rows (they are provided by preserved part), but removes their possible values that might've come as non-null before, and be filtered after join.
Whole optimization rule is split into 2 functions: optimize and pushDown.
    optimize looks for Selection, and when it finds it - it holds Selection until optimize runs on whole sub-tree of Selection. Then it calls pushDown on that subtree, so we can put Selection node in it's correct place.
    pushDown tries to see if Selection can be moved lower. If we still can push lower - we call pushDown with children, and re-create current node with result. If it's a blocker - we construct new Filter (Selection) node, and give it a subtree. Subtree is already fully optimized so it's the best we can do, we don't have to do anything else.

## Pseudocode

def optimize(plan) -> plan:
    when plan:
        Scan -> Scan //  return itself and no optimize further (as scan is lowest i suppose)
        Selection ->
            val optimizedSubtrees = optimize(plan.input) // we call optimize first to get final result from subtree
            pushDown(plan.expr, optimizedSubtrees) // and now we actually try to place selection as low as possible
        Join -> new Join(optimize left, optimize right)
        other -> new Other(optimize(plan.input)) // kinda very high-level. I'll probably have to write explicit branch for each type.

def pushDown(expr, plan) -> plan
    when plan
        Scan, Limit, Selection -> Selection(plan, expr) // here we construct new Filter, and everything else is already under us - we're as low as possible
        Projection ->
            if not createsFilterColumn: Projection(pushDown(expr, plan.input), plan.expt) // keep pushing down
            else: Selection(plan, expr) // we can't go lower because there was no such column.
        Aggregate ->
            if not createsFilterColumn: Aggregate(pushDown())
            else: Selection(plan, expr) // we're filtering on col that aggregate created, e.g count - can't move on.
        InnerJoin ->
            if containsAllColumns(left):
                InnerJoin(pushDown(left plan), right plan)
            elif containsAllColumns(right):
                InnerJoin(left plan, pushDown(right plan))
            else:
                Selection(InnerJoin(left, right))
        LeftJoin ->
            if containsAllColumns(left):
                LeftJoin(pushDown, right plan stays)
            else:
                Selection(Left)
        RightJoin ->
            if containsAllColumns(right):
                RightJoin(left plan stays, pushDown)
            else:
                Selection(Right)
