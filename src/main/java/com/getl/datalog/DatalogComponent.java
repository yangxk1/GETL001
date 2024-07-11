package com.getl.datalog;

import cn.hutool.core.util.NumberUtil;
import com.getl.api.GraphAPI;
import com.getl.datalog.math.cc.MathExpressionParser;
import com.getl.datalog.math.cc.ParseException;
import lombok.ToString;
import org.apache.commons.lang.StringUtils;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.structure.T;

import java.util.*;

public class DatalogComponent {

    public static class Program {
        public List<Rule> rules;
        public List<Fact> facts;

        public Program() {
        }

        public Program(List<Rule> rules, List<Fact> facts) {
            this.rules = rules;
            this.facts = facts;
        }

        @Override
        public String toString() {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append("Program{\n" +
                    "rules:\n");
            rules.forEach(i -> {
                stringBuilder.append(i).append("\n");
            });
            stringBuilder.append("facts:\n");
            facts.forEach(i -> {
                stringBuilder.append(i).append("\n");
            });
            stringBuilder.append("}\n");
            return stringBuilder.toString();
        }
    }

    public abstract static class Statement {
    }

    public static class Rule extends Statement {
        public Head head;
        public Body body;
        public Map<String, Integer> bodyPredicateCounts = new HashMap<>();

        public Rule() {
        }

        public Rule(Head head, Body body) {
            this.head = head;
            this.body = body;
        }

        @Override
        public String toString() {
            return head +
                    " :- " +
                    body;
        }

        public List<Expression> getBody() {
            return body.expressions;
        }
    }

    public static class Fact extends Statement {
        public static Fact buildFact(String predicate, List<String> terms) {
            Expression expression1 = Expression.buildExpression(predicate, terms);
            Fact fact = new Fact(expression1);
            return fact;
        }

        public Expression expression;

        public Fact(Expression expression) {
            this.expression = expression;
        }

        public Expression getExpression() {
            return expression;
        }

        @Override
        public String toString() {
            return expression.toString();
        }
    }

    public static class Head {
        public Expression expression;

        public Head(Expression expression) {
            this.expression = expression;
        }

        @Override
        public String toString() {
            return expression.toString();
        }
    }

    public static class Body {
        public List<Expression> expressions;
        public List<JoinCondition> joinConditions;

        public Body(List<Expression> expressions) {
            this.expressions = expressions;
        } // For facts (no join conditions


        public Body(List<Expression> expressions, List<JoinCondition> joinConditions) {
            this.expressions = expressions;
            this.joinConditions = joinConditions;
        }

        @Override
        public String toString() {
            return String.join(" , ", expressions);
        }
    }

    @ToString
    public static class JoinCondition {
        public String variableName;
        public List<Tuple<Expression, Integer>> tupleList;
        public Tuple<Expression, Integer> constantTuple;

        public JoinCondition(String variableName, List<Tuple<Expression, Integer>> tupleList) {
            this.variableName = variableName;
            this.tupleList = tupleList;
            this.constantTuple = null;
        }

        public JoinCondition(String variableName, Tuple<Expression, Integer> constantTuple) {
            this.variableName = variableName;
            this.tupleList = null;
            this.constantTuple = constantTuple;
        }
    }

    public static class Expression extends Term implements CharSequence {
        public String predicate;
        public List<Term> terms;
        public String alias;
        private boolean negated = false;

        public static Expression buildExpression(String predicate, List<String> terms) {
            int i = 0;
            List<Term> termArrayList = new ArrayList<>();
            for (String term : terms) {
                if (term.matches("[A-Z]([a-z,A-Z,0-9,_])*")) {
                    termArrayList.add(new Variable(term, predicate, i++));
                } else if (term.matches("[a-z,0-9]([a-z,A-Z,0-9,_])*")) {
                    //常量
                    termArrayList.add(new Constant(term, predicate, i++));
                } else {
                    termArrayList.add(new Math(term, predicate, i++));
                }
            }
            return new Expression(predicate, termArrayList, "");
        }

        public static Expression buildExpression(String predicate, List<String> terms, AggreExpress aggreExpress) {
            Expression expression = buildExpression(predicate, terms);
            expression.terms.add(aggreExpress);
            return expression;
        }

        public Expression(String predicate, List<Term> terms, String alias) {
            super(predicate, predicate, 0);
            this.predicate = predicate;
            this.terms = terms;
            this.alias = alias;
        }

        public boolean isNegated() {
            return negated;
        }

        public List<Expression> aggregate(Collection<Map<String, String>> bindings) {
            //默认最后一个是聚合函数
            Term lastTerm = this.terms.get(terms.size() - 1);
            if (!(lastTerm instanceof AggreExpress)) {
                return null;
            }
            Map<String, Integer> cache = new HashMap<>();
            AggreExpress aggreExpress = (AggreExpress) lastTerm;
            int ans = aggreExpress.variable_init;
            bindings.forEach(binding -> {
                List<String> variableStr = new ArrayList<>();
                for (Term term : this.terms.subList(0, terms.size() - 1)) {
                    String value;
                    if (term instanceof Variable) {
                        value = binding.get(term.name);
                        if (value == null) {
                            value = term.name;
                        }
                    } else if (term instanceof Math) {
                        value = ((Math) term).execute(binding);
                    } else {
                        value = term.name;
                    }
                    variableStr.add(value);
                }
                String join = String.join("_,_", variableStr);
                Integer lastCompute = cache.get(join);
                int i = lastCompute == null ? ans : lastCompute;
                i = aggreExpress.eval(i, binding);
                cache.put(join, i);
            });
            List<Expression> expressionList = new ArrayList<>();
            for (Map.Entry<String, Integer> stringIntegerEntry : cache.entrySet()) {
                Expression that = new Expression(this.predicate, new ArrayList<>(), "");
                String[] split = stringIntegerEntry.getKey().split("_,_");
                int i = 0;
                for (String s : split) {
                    that.terms.add(new Term(s, predicate, i++));
                }
                String aggregateValue = stringIntegerEntry.getValue().toString();
                that.terms.add(new Term(aggregateValue, predicate, i++));
                expressionList.add(that);
            }
            return expressionList;
        }

        /**
         * Substitutes the variables in this expression with bindings from a unification.
         *
         * @param bindings The bindings to substitute.
         * @return A new expression with the variables replaced with the values in bindings.
         */
        public Expression substitute(Map<String, String> bindings) {
            // that.terms.add() below doesn't work without the new ArrayList()
            //TODO alias
            Expression that = new Expression(this.predicate, new ArrayList<>(), "");
            that.negated = negated;
            int i = 0;
            for (Term term : this.terms) {
                String value;
                if (term instanceof Variable) {
                    value = bindings.get(term.name);
                    if (value == null) {
                        value = term.name;
                    }
                } else if (term instanceof Math) {
                    value = ((Math) term).execute(bindings);
                } else {
                    value = term.name;
                }
                that.terms.add(new Term(value, predicate, i++));
            }
            return that;
        }

        /**
         * 判断是否为内置 = == != < >
         *
         * @return
         */
        public boolean isBuiltIn() {
            return "=" .equals(predicate) ||
                    "==" .equals(predicate) ||
                    "!=" .equals(predicate) ||
                    "<" .equals(predicate) ||
                    ">" .equals(predicate);
        }

        public boolean isGraphOpt() {
            String pre = this.predicate;
            return pre.equalsIgnoreCase("gremlin");
        }

        public void evalGremlin(GraphAPI graphAPI, Collection<Map<String, String>> bindings) {
            GremlinBlock gremlinBlock = (GremlinBlock) this;
            if (gremlinBlock.result != null) {
                bindings.addAll(gremlinBlock.result);
                return;
            }
            List<String> stepLabels = new ArrayList<>();
            for (Term term : gremlinBlock.terms) {
                stepLabels.add(term.name);
            }
            GraphTraversal graphTraversal = gremlinBlock.graphTraversal;
            if (stepLabels.isEmpty()) {

            } else if (stepLabels.size() == 1) {
                graphTraversal.select(stepLabels.get(0));
            } else if (stepLabels.size() == 2) {
                graphTraversal.select(stepLabels.get(0), stepLabels.get(1));
            } else {
                graphTraversal.select(stepLabels.get(0), stepLabels.get(1), stepLabels.subList(2, stepLabels.size()).toArray(new String[0]));
            }
            for (String pop : gremlinBlock.pops) {
                if ("_" .equals(pop)) {
                    graphTraversal.by(T.id);
                } else {
                    graphTraversal.by(pop);
                }
            }
            List ans = new ArrayList();
            List<Map<String, Object>> list = graphTraversal.toList();
            for (Map<String, Object> stringObjectMap : list) {
                Map<String, String> newMap = new HashMap<>();
                for (Map.Entry<String, Object> entry : stringObjectMap.entrySet()) {
                    String key = entry.getKey();
                    Object value = entry.getValue();
                    String stringValue = String.valueOf(value); // 将值转换为 String 类型
                    newMap.put(key, stringValue);
                }
                ans.add(newMap);
            }
            bindings.addAll(ans);
            gremlinBlock.result = ans;
            return;
        }

        /**
         * 计算内置谓词
         */
        public boolean evalBuiltIn(Map<String, String> bindings) {
            //TODO 根据依赖排序
            //赋值符号 " = "
            Term leftTerm = terms.get(0);
            Term rightTerm = terms.get(1);
            //如果rightTerm
            if ("=" .equals(predicate)) {
                //暂且认为左边为变量
                // '=' is special
                assert leftTerm instanceof Variable;
                String rightValue = null;
                try {
                    if (rightTerm instanceof Math) {
                        rightValue = NumberUtil.toStr(new MathExpressionParser(rightTerm.name).parse(bindings));
                    } else if (rightTerm instanceof Variable) {
                        rightValue = bindings.get(leftTerm.name);
                    } else if (rightTerm instanceof Constant) {
                        rightValue = rightTerm.name;
                    }
                } catch (ParseException e) {
                    System.out.println(e);
                }
                bindings.put(leftTerm.name, rightValue);
                return true;
            }
            String leftValue = "";
            try {
                if (leftTerm instanceof Math) {
                    leftValue = NumberUtil.toStr(new MathExpressionParser(rightTerm.name).parse(bindings));
                } else if (leftTerm instanceof Variable) {
                    leftValue = bindings.get(leftTerm.name);
                } else if (leftTerm instanceof Constant) {
                    leftValue = leftTerm.name;
                }
            } catch (ParseException e) {
                System.out.println(e);
            }
            if ("==" .equals(predicate)) {
                try {
                    if (rightTerm instanceof Math) {
                        double right = new MathExpressionParser(rightTerm.name).parse(bindings);
                        double left = NumberUtil.parseDouble(leftValue);
                        return NumberUtil.equals(right, left);
                    } else if (rightTerm instanceof Variable) {
                        return leftValue.equals(bindings.get(leftTerm.name));
                    } else if (rightTerm instanceof Constant) {
                        return leftValue.equals(leftTerm.name);
                    }
                } catch (ParseException e) {
                    System.out.println(e);
                }
                return false;
            }
            if ("!=" .equals(predicate)) {
                try {
                    if (rightTerm instanceof Math) {
                        double right = new MathExpressionParser(rightTerm.name).parse(bindings);
                        double left = NumberUtil.parseDouble(leftValue);
                        return !NumberUtil.equals(right, left);
                    } else if (rightTerm instanceof Variable) {
                        return !leftValue.equals(bindings.get(leftTerm.name));
                    } else if (rightTerm instanceof Constant) {
                        return !leftValue.equals(leftTerm.name);
                    }
                } catch (ParseException e) {
                    System.out.println(e);
                }
                return false;
            }
            if ("<" .equals(predicate)) {
                try {
                    double left = NumberUtil.parseDouble(leftValue);
                    if (rightTerm instanceof Math) {
                        double right = new MathExpressionParser(rightTerm.name).parse(bindings);
                        return left < right;
                    } else if (rightTerm instanceof Variable) {
                        return left < NumberUtil.parseDouble(bindings.get(leftTerm.name));
                    } else if (rightTerm instanceof Constant) {
                        return left < NumberUtil.parseDouble(leftTerm.name);
                    }
                } catch (ParseException e) {
                    System.out.println(e);
                }
                return false;
            }
            if (">" .equals(predicate)) {
                try {
                    double left = NumberUtil.parseDouble(leftValue);
                    if (rightTerm instanceof Math) {
                        double right = new MathExpressionParser(rightTerm.name).parse(bindings);
                        return left > right;
                    } else if (rightTerm instanceof Variable) {
                        return left > NumberUtil.parseDouble(bindings.get(leftTerm.name));
                    } else if (rightTerm instanceof Constant) {
                        return left > NumberUtil.parseDouble(leftTerm.name);
                    }
                } catch (ParseException e) {
                    System.out.println(e);
                }
                return false;
            }
            return false;
        }

        public boolean unify(Expression goal, Map<String, String> bindings) {
            if (!this.predicate.equals(goal.predicate) || this.arity() != goal.arity()) {
                return false;
            }
            for (int i = 0; i < this.arity(); i++) {
                Term term1 = this.terms.get(i);
                Term term2 = goal.terms.get(i);
                if (term1 instanceof Variable) {
                    //this(X,...)
                    if (!term1.equals(term2)) {
                        //goal(x,...)
                        if (!bindings.containsKey(term1.name)) {
                            bindings.put(term1.name, term2.name);
                        } else if (!bindings.get(term1.name).equals(term2.name)) {
                            return false;
                        }
                    }
                } else if (term2 instanceof Variable) {
                    //goal(X,...)
                    if (!bindings.containsKey(term2.name)) {
                        bindings.put(term2.name, term1.name);
                    } else if (!bindings.get(term2.name).equals(term1.name)) {
                        return false;
                    }
                } else if (!term1.name.equals(term2.name)) {
                    return false;
                }
            }
            return true;
        }

        public int arity() {
            return terms.size();
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Expression that = (Expression) o;
            return Objects.equals(predicate, that.predicate) && Objects.equals(terms, that.terms);
        }

        @Override
        public int hashCode() {
            return Objects.hash(predicate, terms);
        }

        @Override
        public int length() {
            return toString().length();
        }

        @Override
        public char charAt(int index) {
            return toString().charAt(index);
        }

        @Override
        public CharSequence subSequence(int start, int end) {
            return toString().subSequence(start, end);
        }

        @Override
        public String toString() {
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(predicate).append("(");
            for (int i = 0; i < terms.size(); i++) {
                if (i != 0) {
                    stringBuilder.append(",");
                }
                stringBuilder.append(terms.get(i));
            }
            stringBuilder.append(")");
            return stringBuilder.toString();
        }
    }

    public static class Term {
        public String name;
        public String source;
        public int index;

        public Term(String name, String source, int index) {
            this.name = name;
            this.source = source;
            this.index = index;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            Term term = (Term) o;
            return Objects.equals(name, term.name);
        }

        @Override
        public int hashCode() {
            return Objects.hash(name);
        }

        @Override
        public String toString() {
            return name;
        }
    }

    public static class GremlinBlock extends Expression {
        GraphTraversal graphTraversal;

        private List<String> pops;
        String gremlinBlock;

        Collection<Map<String, String>> result = null;

        public static GremlinBlock buildGremlinBlock(GraphTraversal graphTraversal, List<String> terms, List<String> pops) {
            int i = 0;
            if (terms.size() != pops.size()) {
                throw new RuntimeException("参数数目不一致");
            }
            List<Term> termArrayList = new ArrayList<>();
            for (String term : terms) {
                termArrayList.add(new Variable(term, "gremlin", i++));
            }
            GremlinBlock gremlinBlock1 = new GremlinBlock("gremlin", termArrayList, "");
            gremlinBlock1.graphTraversal = graphTraversal;
            gremlinBlock1.pops = pops;
            return gremlinBlock1;
        }

        public GremlinBlock(String gremlinBlock, List<Term> terms, String alias) {
            super("gremlin", terms, alias);
        }
    }

    public abstract static class AggreExpress extends Expression {
        @Override
        public String toString() {
            if (this instanceof Max) {
                return "max[" + terms.get(0).name + "]";
            } else if (this instanceof Min) {
                return "min[" + terms.get(0).name + "]";
            } else if (this instanceof Sum) {
                return "sum[" + terms.get(0).name + "]";
            } else if (this instanceof Count) {
                return "count[" + terms.get(0).name + "]";
            }
            return "AGGREGATE[" + terms.get(0).name + "]";
        }

        public AggreExpress(String predicate, List<Term> terms, String alias) {
            super(predicate, terms, alias);
        }

        public int variable_init = 0;

        public abstract int eval(int i, Map<String, String> binding);

        public static class Max extends AggreExpress {
            public Max(String predicate, List<Term> terms, String alias) {
                super(predicate, terms, alias);
                variable_init = Integer.MIN_VALUE;
            }

            public Max(String termName) {
                this(null, Collections.singletonList(new Term(termName, "count", 0)), null);
            }

            @Override
            public int eval(int i, Map<String, String> binding) {
                Term term = terms.get(0);
                String value = binding.get(term.name);
                Integer number = NumberUtil.parseInt(value, variable_init);
                return NumberUtil.max(i, number);
            }
        }

        public static class Min extends AggreExpress {
            public Min(String predicate, List<Term> terms, String alias) {
                super(predicate, terms, alias);
                variable_init = Integer.MAX_VALUE;
            }

            public Min(String termName) {
                this(null, Collections.singletonList(new Term(termName, "count", 0)), null);
            }

            @Override
            public int eval(int i, Map<String, String> binding) {
                Term term = terms.get(0);
                String value = binding.get(term.name);
                Integer number = NumberUtil.parseInt(value, variable_init);
                return NumberUtil.min(i, number);
            }
        }

        public static class Sum extends AggreExpress {
            public Sum(String predicate, List<Term> terms, String alias) {
                super(predicate, terms, alias);
                variable_init = 0;
            }

            public Sum(String termName) {
                this(null, Collections.singletonList(new Term(termName, "count", 0)), null);
            }

            @Override
            public int eval(int i, Map<String, String> binding) {
                Term term = terms.get(0);
                String value = binding.get(term.name);
                Integer number = NumberUtil.parseInt(value, variable_init);
                return i + number;
            }
        }

        public static class Count extends AggreExpress {
            public Count(String predicate, List<Term> terms, String alias) {
                super(predicate, terms, alias);
                variable_init = 0;
            }

            public Count(String termName) {
                this(null, Collections.singletonList(new Term(termName, "count", 0)), null);
            }

            @Override
            public int eval(int i, Map<String, String> binding) {
                Term term = terms.get(0);
                String value = binding.get(term.name);
                if (StringUtils.isNotBlank(value)) {
                    i++;
                }
                return i;
            }
        }
    }

    public static class Variable extends Term {

        public Variable(String name, String source, int index) {
            super(name, source, index);
        }
    }

    public static class Math extends Term {

        public Math(String name, String source, int index) {
            super(name, source, index);
        }

        public String execute(Map<String, String> binding) {
            try {
                return NumberUtil.toStr(new MathExpressionParser(name).parse(binding));
            } catch (ParseException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public static class Constant extends Term {

        public Constant(String value) {
            super(value, null, 0);
        }

        public Constant(String value, String source, int index) {
            super(value, source, index);
        }
    }

    //Cause Java doesn't have tuples
    public static class Tuple<A, B> {
        public A first;
        public B second;

        public Tuple(A first, B second) {
            this.first = first;
            this.second = second;
        }
    }

    public static class Triple<A, B, C> {
        public A first;
        public B second;
        public C third;

        public Triple(A first, B second, C third) {
            this.first = first;
            this.second = second;
            this.third = third;
        }
    }


}
