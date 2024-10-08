/* Generated By:JavaCC: Do not edit this line. MathExpressionParser.java */
package com.getl.datalog.math.cc;

import com.getl.datalog.math.NumberParserUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MathExpressionParser implements MathExpressionParserConstants {

    public static void main(String[] args) throws ParseException {
        System.out.println(Math.max(Double.MIN_VALUE, 11));
        String express = "(A)";
        Map<String, String> bingings = new HashMap<>();
        bingings.put("A", "2");
        bingings.put("V", "v");
//    bingings.put("B",7D);
//    bingings.put("C",11D);
        System.out.println(new MathExpressionParser(express).parse(bingings));
    }

    public MathExpressionParser(String expression) {
        this(new java.io.StringReader(expression));
    }

    /**
     * 解析表达式
     */
    final public double parse(Map<String, String> bindings) throws ParseException {
        Token t;
        double result = 0;
        double i = 0;
        result = multiplieAndDivide(bindings);
        label_1:
        while (true) {
            switch ((jj_ntk == -1) ? jj_ntk() : jj_ntk) {
                case PLUS:
                case MINUS:
                    ;
                    break;
                default:
                    jj_la1[0] = jj_gen;
                    break label_1;
            }
            switch ((jj_ntk == -1) ? jj_ntk() : jj_ntk) {
                case PLUS:
                    jj_consume_token(PLUS);
                    i = multiplieAndDivide(bindings);
                    result += i;
                    break;
                case MINUS:
                    jj_consume_token(MINUS);
                    i = multiplieAndDivide(bindings);
                    result -= i;
                    break;
                default:
                    jj_la1[1] = jj_gen;
                    jj_consume_token(-1);
                    throw new ParseException();
            }
        }
        {
            if (true) return result;
        }
        throw new Error("Missing return statement in function");
    }

    /**
     * 基本运算单元
     **/
    final public double primary(Map<String, String> bindings) throws ParseException {
        Token t;
        double d;
        List calList;
        d = doNumber(bindings);
        {
            if (true) return d;
        }
        throw new Error("Missing return statement in function");
    }

    /**
     * 读取数字 乘除算
     **/
    final public double multiplieAndDivide(Map<String, String> bindings) throws ParseException {
        double result = 0;
        double i = 0;
        result = primary(bindings);
        label_2:
        while (true) {
            switch ((jj_ntk == -1) ? jj_ntk() : jj_ntk) {
                case MULTIPLIE:
                case DIVIDE:
                    ;
                    break;
                default:
                    jj_la1[2] = jj_gen;
                    break label_2;
            }
            switch ((jj_ntk == -1) ? jj_ntk() : jj_ntk) {
                case MULTIPLIE:
                    jj_consume_token(MULTIPLIE);
                    i = primary(bindings);
                    result *= i;
                    break;
                case DIVIDE:
                    jj_consume_token(DIVIDE);
                    i = primary(bindings);
                    result /= i;
                    break;
                default:
                    jj_la1[3] = jj_gen;
                    jj_consume_token(-1);
                    throw new ParseException();
            }
        }
        {
            if (true) return result;
        }
        throw new Error("Missing return statement in function");
    }

    /**
     * 函数计算
     **/
    final public double doFunction(Map<String, String> binings) throws ParseException {
        List<Double> args;
        switch ((jj_ntk == -1) ? jj_ntk() : jj_ntk) {
            case MAX:
                jj_consume_token(MAX);
                args = calculatorPar(binings);
                double var = Double.MIN_VALUE;
                for (double arg : args) {
                    var = Math.max(var, arg);
                }
            {
                if (true) return var;
            }
            break;
            case MIN:
                jj_consume_token(MIN);
                args = calculatorPar(binings);
                double var1 = Double.MAX_VALUE;
                for (double arg : args) {
                    var1 = Math.min(var1, arg);
                }
            {
                if (true) return var1;
            }
            break;
            case SUM:
                jj_consume_token(SUM);
                args = calculatorPar(binings);
                double var2 = 0;
                for (double arg : args) {
                    var2 += arg;
                }
            {
                if (true) return var2;
            }
            break;
            default:
                jj_la1[4] = jj_gen;
                jj_consume_token(-1);
                throw new ParseException();
        }
        throw new Error("Missing return statement in function");
    }

    final public double doNumber(Map<String, String> binings) throws ParseException {
        Token t;
        double d;
        List<Double> calList;
        double result;
        result = getNumber(binings);
        label_3:
        while (true) {
            switch ((jj_ntk == -1) ? jj_ntk() : jj_ntk) {
                case OPEN_PAR:
                case MIN:
                case MAX:
                case SUM:
                    ;
                    break;
                default:
                    jj_la1[5] = jj_gen;
                    break label_3;
            }
            switch ((jj_ntk == -1) ? jj_ntk() : jj_ntk) {
                case OPEN_PAR:
                    calList = calculatorPar(binings);
                    d = calList.get(0);
                    result = d * result;
                    break;
                case MIN:
                case MAX:
                case SUM:
                    d = doFunction(binings);
                    result = d * result;
                    break;
                default:
                    jj_la1[6] = jj_gen;
                    jj_consume_token(-1);
                    throw new ParseException();
            }
        }
        {
            if (true) return result;
        }

        {
            if (true) return result;
        }
        throw new Error("Missing return statement in function");
    }

    /**
     * 如果是数字则直接返回数字，否则，是变量则返回变量的次方
     **/
    final public double getNumber(Map<String, String> binings) throws ParseException {
        Token t;
        double result = 1;
        double var;
        switch ((jj_ntk == -1) ? jj_ntk() : jj_ntk) {
            case NUMBER:
                t = jj_consume_token(NUMBER);
                result = Double.parseDouble(t.image);
            {
                if (true) return result;
            }
            break;
            default:
                jj_la1[8] = jj_gen;
                label_4:
                while (true) {
                    switch ((jj_ntk == -1) ? jj_ntk() : jj_ntk) {
                        case VAR_NAME:
                            ;
                            break;
                        default:
                            jj_la1[7] = jj_gen;
                            break label_4;
                    }
                    t = jj_consume_token(VAR_NAME);
                    var = NumberParserUtils.parse(binings.get(t.image));
                    result *= var;
                }
            {
                if (true) return result;
            }
        }
        throw new Error("Missing return statement in function");
    }

    /*计算圆弧内的表达式*/
    final public List<Double> calculatorPar(Map<String, String> binings) throws ParseException {
        List<Double> ans = new ArrayList();
        double d;
        jj_consume_token(OPEN_PAR);
        d = parse(binings);
        ans.add(d);
        label_5:
        while (true) {
            switch ((jj_ntk == -1) ? jj_ntk() : jj_ntk) {
                case COMMA:
                    ;
                    break;
                default:
                    jj_la1[9] = jj_gen;
                    break label_5;
            }
            jj_consume_token(COMMA);
            d = parse(binings);
            ans.add(d);
        }
        jj_consume_token(CLOSE_PAR);
        {
            if (true) return ans;
        }
        throw new Error("Missing return statement in function");
    }

    /**
     * Generated Token Manager.
     */
    public MathExpressionParserTokenManager token_source;
    SimpleCharStream jj_input_stream;
    /**
     * Current token.
     */
    public Token token;
    /**
     * Next token.
     */
    public Token jj_nt;
    private int jj_ntk;
    private int jj_gen;
    final private int[] jj_la1 = new int[10];
    static private int[] jj_la1_0;

    static {
        jj_la1_init_0();
    }

    private static void jj_la1_init_0() {
        jj_la1_0 = new int[]{0x30, 0x30, 0xc0, 0xc0, 0x3800, 0x3900, 0x3900, 0x4000, 0x8, 0x8000,};
    }

    /**
     * Constructor with InputStream.
     */
    public MathExpressionParser(java.io.InputStream stream) {
        this(stream, null);
    }

    /**
     * Constructor with InputStream and supplied encoding
     */
    public MathExpressionParser(java.io.InputStream stream, String encoding) {
        try {
            jj_input_stream = new SimpleCharStream(stream, encoding, 1, 1);
        } catch (java.io.UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        token_source = new MathExpressionParserTokenManager(jj_input_stream);
        token = new Token();
        jj_ntk = -1;
        jj_gen = 0;
        for (int i = 0; i < 10; i++) jj_la1[i] = -1;
    }

    /**
     * Reinitialise.
     */
    public void ReInit(java.io.InputStream stream) {
        ReInit(stream, null);
    }

    /**
     * Reinitialise.
     */
    public void ReInit(java.io.InputStream stream, String encoding) {
        try {
            jj_input_stream.ReInit(stream, encoding, 1, 1);
        } catch (java.io.UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
        token_source.ReInit(jj_input_stream);
        token = new Token();
        jj_ntk = -1;
        jj_gen = 0;
        for (int i = 0; i < 10; i++) jj_la1[i] = -1;
    }

    /**
     * Constructor.
     */
    public MathExpressionParser(java.io.Reader stream) {
        jj_input_stream = new SimpleCharStream(stream, 1, 1);
        token_source = new MathExpressionParserTokenManager(jj_input_stream);
        token = new Token();
        jj_ntk = -1;
        jj_gen = 0;
        for (int i = 0; i < 10; i++) jj_la1[i] = -1;
    }

    /**
     * Reinitialise.
     */
    public void ReInit(java.io.Reader stream) {
        jj_input_stream.ReInit(stream, 1, 1);
        token_source.ReInit(jj_input_stream);
        token = new Token();
        jj_ntk = -1;
        jj_gen = 0;
        for (int i = 0; i < 10; i++) jj_la1[i] = -1;
    }

    /**
     * Constructor with generated Token Manager.
     */
    public MathExpressionParser(MathExpressionParserTokenManager tm) {
        token_source = tm;
        token = new Token();
        jj_ntk = -1;
        jj_gen = 0;
        for (int i = 0; i < 10; i++) jj_la1[i] = -1;
    }

    /**
     * Reinitialise.
     */
    public void ReInit(MathExpressionParserTokenManager tm) {
        token_source = tm;
        token = new Token();
        jj_ntk = -1;
        jj_gen = 0;
        for (int i = 0; i < 10; i++) jj_la1[i] = -1;
    }

    private Token jj_consume_token(int kind) throws ParseException {
        Token oldToken;
        if ((oldToken = token).next != null) token = token.next;
        else token = token.next = token_source.getNextToken();
        jj_ntk = -1;
        if (token.kind == kind) {
            jj_gen++;
            return token;
        }
        token = oldToken;
        jj_kind = kind;
        throw generateParseException();
    }


    /**
     * Get the next Token.
     */
    final public Token getNextToken() {
        if (token.next != null) token = token.next;
        else token = token.next = token_source.getNextToken();
        jj_ntk = -1;
        jj_gen++;
        return token;
    }

    /**
     * Get the specific Token.
     */
    final public Token getToken(int index) {
        Token t = token;
        for (int i = 0; i < index; i++) {
            if (t.next != null) t = t.next;
            else t = t.next = token_source.getNextToken();
        }
        return t;
    }

    private int jj_ntk() {
        if ((jj_nt = token.next) == null)
            return (jj_ntk = (token.next = token_source.getNextToken()).kind);
        else
            return (jj_ntk = jj_nt.kind);
    }

    private java.util.List jj_expentries = new java.util.ArrayList();
    private int[] jj_expentry;
    private int jj_kind = -1;

    /**
     * Generate ParseException.
     */
    public ParseException generateParseException() {
        jj_expentries.clear();
        boolean[] la1tokens = new boolean[16];
        if (jj_kind >= 0) {
            la1tokens[jj_kind] = true;
            jj_kind = -1;
        }
        for (int i = 0; i < 10; i++) {
            if (jj_la1[i] == jj_gen) {
                for (int j = 0; j < 32; j++) {
                    if ((jj_la1_0[i] & (1 << j)) != 0) {
                        la1tokens[j] = true;
                    }
                }
            }
        }
        for (int i = 0; i < 16; i++) {
            if (la1tokens[i]) {
                jj_expentry = new int[1];
                jj_expentry[0] = i;
                jj_expentries.add(jj_expentry);
            }
        }
        int[][] exptokseq = new int[jj_expentries.size()][];
        for (int i = 0; i < jj_expentries.size(); i++) {
            exptokseq[i] = (int[]) jj_expentries.get(i);
        }
        return new ParseException(token, exptokseq, tokenImage);
    }

    /**
     * Enable tracing.
     */
    final public void enable_tracing() {
    }

    /**
     * Disable tracing.
     */
    final public void disable_tracing() {
    }

}
