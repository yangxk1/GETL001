package com.getl.datalog;

import cn.hutool.core.collection.CollectionUtil;
import com.google.common.collect.HashMultimap;
import com.getl.api.GraphAPI;

import java.util.*;
import java.util.stream.Collectors;

public class Query {
    private DatalogComponent.Program program;
    private GraphAPI graph;

    public Query(DatalogComponent.Program program, GraphAPI graph) {
        this.program = program;
        this.graph = graph;
    }

    public Collection<Map<String, String>> query(Map<String, String> bindings, DatalogComponent.Expression... goals) {
        return query(Arrays.asList(goals), bindings);
    }

    public Collection<Map<String, String>> query(List<DatalogComponent.Expression> goals, Map<String, String> bindings) {
//        if (goals.isEmpty()) {
//            return Collections.emptyList();
//        }

        Collection<String> predicates = getRelevantPredicates(goals);
        List<DatalogComponent.Rule> rules = new ArrayList<>(this.program.rules.size());
        CollectionUtil.addAll(rules, this.program.rules);

        // Build an IndexedSet<> with only the relevant facts for this particular query.
        HashMultimap<String, DatalogComponent.Expression> facts = HashMultimap.create();
        for (DatalogComponent.Fact fact : this.program.facts) {
            facts.put(fact.expression.predicate, fact.expression);
        }

        // Build the database. A Set ensures that the facts are unique
        HashMultimap<String, DatalogComponent.Expression> resultSet = expandDatabase(graph, facts, rules);
        this.program.facts = resultSet.values().stream().map(DatalogComponent.Fact::new).collect(Collectors.toList());
        // Now match the expanded database to the goals
        return matchGoals(goals, resultSet, bindings);
    }

    private Collection<Map<String, String>> matchGoals(List<DatalogComponent.Expression> goals, HashMultimap<String, DatalogComponent.Expression> facts, Map<String, String> bindings) {
        DatalogComponent.Expression goal = goals.get(0);

        boolean lastGoal = (goals.size() == 1);

        if (goal.isBuiltIn()) {
            Map<String, String> newBindings = new HashMap<>(bindings);
            boolean eval = goal.evalBuiltIn(newBindings);
            if (eval && !goal.isNegated() || !eval && goal.isNegated()) {
                if (lastGoal) {
                    return Collections.singletonList(newBindings);
                } else {
                    return matchGoals(goals.subList(1, goals.size()), facts, newBindings);
                }
            }
            return Collections.emptyList();
        } else if (goal.isGraphOpt()) {
            Collection<Map<String, String>> answers = new ArrayList<>();
            Collection<Map<String, String>> gremlinFact = new ArrayList<>();
            goal.evalGremlin(graph, gremlinFact);
            for (Map<String, String> gremlinBindings : gremlinFact) {
                Map<String, String> newBindings = new HashMap<>(bindings);
                if (unify(gremlinBindings, newBindings)) {
                    if (lastGoal) {
                        answers.add(newBindings);
                    } else {
                        answers.addAll(matchGoals(goals.subList(1, goals.size()), facts, newBindings));
                    }
                }
            }
            return answers;
        }

        Collection<Map<String, String>> answers = new ArrayList<>();
        if (!goal.isNegated()) {
            //head(X,Y,...) :- goal(X,Y,...),nextGoal(X,Y,...),...
            //find the fact with same predicate
            for (DatalogComponent.Expression fact : facts.get(goal.predicate)) {
                Map<String, String> newBindings = new HashMap<>(bindings);
                if (fact.unify(goal, newBindings)) {
                    if (lastGoal) {
                        answers.add(newBindings);
                    } else {
                        // More goals to match. Recurse with the remaining goals.
                        answers.addAll(matchGoals(goals.subList(1, goals.size()), facts, newBindings));
                    }
                }
            }
        } else {
            // Negated rule: If you find any fact that matches the goal, then the goal is false.
            // See definition 4.3.2 of [bra2] and section VI-B of [ceri].
            // Substitute the bindings in the rule first.
            // If your rule is `und(X) :- stud(X), not grad(X)` and you're at the `not grad` part, and in the
            // previous goal stud(a) was true, then bindings now contains X:a so we want to search the database
            // for the fact grad(a).
            if (bindings != null) {
                goal = goal.substitute(bindings);
            }
            for (DatalogComponent.Expression fact : facts.get(goal.predicate)) {
                Map<String, String> newBindings = new HashMap<>(bindings);
                if (fact.unify(goal, newBindings)) {
                    return Collections.emptyList();
                }
            }
            // not found
            if (lastGoal) {
                answers.add(bindings);
            } else {
                answers.addAll(matchGoals(goals.subList(1, goals.size()), facts, bindings));
            }
        }
        return answers;
    }

    private boolean unify(Map<String, String> gremlinBindings, Map<String, String> newBindings) {
        //goal(X,...)
        for (Map.Entry<String, String> stringStringEntry : gremlinBindings.entrySet()) {
            if (!newBindings.containsKey(stringStringEntry.getKey())) {
                newBindings.put(stringStringEntry.getKey(), stringStringEntry.getValue());
            } else if (!stringStringEntry.getValue().equals(newBindings.get(stringStringEntry.getKey()))) {
                return false;
            }
        }
        return true;
    }

    private HashMultimap<String, DatalogComponent.Expression> expandDatabase(GraphAPI graph, HashMultimap<String, DatalogComponent.Expression> facts, Collection<DatalogComponent.Rule> rules) {
        List<Collection<DatalogComponent.Rule>> strata = computeStratification(rules);
        for (int i = 0; i < strata.size(); i++) {
            Collection<DatalogComponent.Rule> _rules = strata.get(i);
            expandStrata(facts, _rules);
        }
        //TODO 数据库查询
        return facts;
    }

    private Collection<DatalogComponent.Expression> expandStrata(HashMultimap<String, DatalogComponent.Expression> facts, Collection<DatalogComponent.Rule> strataRules) {

        if (strataRules == null || strataRules.isEmpty()) {
            return Collections.emptyList();
        }

        Collection<DatalogComponent.Rule> rules = strataRules;

        Map<String, Collection<DatalogComponent.Rule>> dependentRules = buildDependentRules(strataRules);

        while (true) {
            // Match each rule to the facts
            HashMultimap<String, DatalogComponent.Expression> newFacts = HashMultimap.create();
            for (DatalogComponent.Rule rule : rules) {
                newFacts.putAll(matchRule(facts, rule));
            }

            // Repeat until there are no more facts added
            if (newFacts.isEmpty()) {
                return facts.values();
            }

            // Determine which rules depend on the newly derived facts
            rules = getDependentRules(newFacts, dependentRules);

            facts.putAll(newFacts);
        }
    }

    private HashMultimap<String, DatalogComponent.Expression> matchRule(HashMultimap<String, DatalogComponent.Expression> facts, DatalogComponent.Rule rule) {
        if (rule.getBody().isEmpty()) {
            return HashMultimap.create();
        }
        // Match the rule body to the facts.
        Collection<Map<String, String>> answers = matchGoals(rule.getBody(), facts, new HashMap<>());
        HashMultimap<String, DatalogComponent.Expression> newFacts = HashMultimap.create();
        List<DatalogComponent.Expression> aggregate = rule.head.expression.aggregate(answers);
        if (aggregate != null) {
            aggregate.stream()
                    .filter(derivedFact -> !facts.containsValue(derivedFact))
                    .forEach(exp -> newFacts.put(exp.predicate, exp));
            return newFacts;
        }
        answers.stream().map(answer -> rule.head.expression.substitute(answer))
                .filter(derivedFact -> !facts.containsValue(derivedFact))
                .forEach(exp -> newFacts.put(exp.predicate, exp));
        return newFacts;
    }

    protected static Collection<DatalogComponent.Rule> getDependentRules(HashMultimap<String, DatalogComponent.Expression> facts, Map<String, Collection<DatalogComponent.Rule>> dependents) {
        Set<DatalogComponent.Rule> dependantRules = new HashSet<>();
        for (String predicate : facts.keySet()) {
            Collection<DatalogComponent.Rule> rules = dependents.get(predicate);
            if (rules != null) {
                dependantRules.addAll(rules);
            }
        }
        return dependantRules;
    }

    protected static Map<String, Collection<DatalogComponent.Rule>> buildDependentRules(Collection<DatalogComponent.Rule> rules) {
        Map<String, Collection<DatalogComponent.Rule>> map = new HashMap<>();
        for (DatalogComponent.Rule rule : rules) {
            for (DatalogComponent.Expression goal : rule.getBody()) {
                Collection<DatalogComponent.Rule> dependants = map.get(goal.predicate);
                if (dependants == null) {
                    dependants = new ArrayList<>();
                    map.put(goal.predicate, dependants);
                }
                if (!dependants.contains(rule))
                    dependants.add(rule);
            }
        }
        return map;
    }

    public static List<Collection<DatalogComponent.Rule>> computeStratification(Collection<DatalogComponent.Rule> allRules) {
        ArrayList<Collection<DatalogComponent.Rule>> strata = new ArrayList<>(10);
        List<DatalogComponent.Rule> gremlinRulers = new ArrayList<>();
        for (DatalogComponent.Rule rule : allRules) {
            if (rule.body.expressions.get(0).isGraphOpt()) {
                gremlinRulers.add(rule);
            }
        }
        if (!gremlinRulers.isEmpty()) {
            strata.add(gremlinRulers);
            allRules.removeAll(gremlinRulers);
        }
        Map<String, Integer> strats = new HashMap<>();
        for (DatalogComponent.Rule rule : allRules) {
            String pred = rule.head.expression.predicate;
            Integer stratum = strats.get(pred);
            if (stratum == null) {
                stratum = depthFirstSearch(rule.head.expression, allRules, new ArrayList<>(), 0);
                strats.put(pred, stratum);
            }
            while (stratum >= strata.size()) {
                strata.add(new ArrayList<>());
            }
            strata.get(stratum).add(rule);
        }

        strata.add(allRules);
        return strata;
    }

    private static int depthFirstSearch(DatalogComponent.Expression goal, Collection<DatalogComponent.Rule> graph, List<DatalogComponent.Expression> visited, int level) {
        String pred = goal.predicate;

        // Step (1): Guard against negative recursion
        boolean negated = goal.isNegated();
        StringBuilder route = new StringBuilder(pred); // for error reporting
        for (int i = visited.size() - 1; i >= 0; i--) {
            DatalogComponent.Expression e = visited.get(i);
            route.append(e.isNegated() ? " <- ~" : " <- ").append(e.predicate);
            if (e.predicate.equals(pred)) {
                if (negated) {
                    throw new RuntimeException("Program is not stratified - predicate " + pred + " has a negative recursion: " + route);
                }
                return 0;
            }
            if (e.isNegated()) {
                negated = true;
            }
        }
        visited.add(goal);

        // Step (2): Do the actual depth-first search to compute the strata
        int m = 0;
        for (DatalogComponent.Rule rule : graph) {
            if (rule.head.expression.predicate.equals(pred)) {
                for (DatalogComponent.Expression expr : rule.getBody()) {
                    int x = depthFirstSearch(expr, graph, visited, level + 1);
                    if (expr.isNegated())
                        x++;
                    if (x > m) {
                        m = x;
                    }
                }
            }
        }
        visited.remove(visited.size() - 1);
        return m;
    }

    //筛选和查询相关的条件
    protected Collection<String> getRelevantPredicates(List<DatalogComponent.Expression> originalGoals) {
        Collection<String> relevant = new HashSet<>();
        LinkedList<DatalogComponent.Expression> goals = new LinkedList<>(originalGoals);
        while (!goals.isEmpty()) {
            DatalogComponent.Expression expr = goals.poll();
            if (!relevant.contains(expr.predicate)) {
                relevant.add(expr.predicate);
                for (DatalogComponent.Rule rule : this.program.rules) {
                    if (rule.head.expression.predicate.equals(expr.predicate)) {
                        goals.addAll(rule.getBody());
                    }
                }
            }
        }
        return relevant;
    }
}
