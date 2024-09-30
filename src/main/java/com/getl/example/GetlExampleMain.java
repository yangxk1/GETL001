package com.getl.example;

import com.getl.example.converter.PG2UGTest;
import com.getl.example.converter.RM2UGTest;
import com.getl.example.query.*;
import com.getl.example.utils.DistributeDataset;
import com.getl.example.utils.LDBCStatistics;
import org.apache.commons.cli.*;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public class GetlExampleMain {

    private static Map<String, String> classMap;

    public static void init() {
        classMap = new HashMap<>();
        classMap.put("q1", "com.getl.example.query.Q1");
        classMap.put("q2", "com.getl.example.query.Q2");
        classMap.put("q3", "com.getl.example.query.Q3");
        classMap.put("q4", "com.getl.example.query.Q4");
        classMap.put("q5", "com.getl.example.query.Q5");
        classMap.put("q6", "com.getl.example.query.Q6");
        classMap.put("q7", "com.getl.example.query.Q7");
        classMap.put("asyncpg2ugtest", "com.getl.example.converter.AsyncPG2UGTest");
        classMap.put("asyncrm2ugtest", "com.getl.example.converter.AsyncRM2UGTest");


    }

    public static void main(String[] args) throws ParseException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        init();
        Options options = new Options();
        options.addOption("c", true, "CLASS NAME");
        options.addOption("r", false, "RDF FILE URL");
        options.addOption("j", false, "JDBC URL");
        options.addOption("ju", false, "JDBC USER NAME");
        options.addOption("jp", false, "JDBC PASSWORD");
        options.addOption("p", false, "PROPERTY GRAPH FILE URL");
        CommandLineParser parser = new DefaultParser();
        parser.parse(options, args);
        CommandLine cmd = parser.parse(options, args);
        if (!cmd.hasOption("c")) {
            throw new RuntimeException("Required parameters -c CLASS NAME");
        }
        String className = classMap.get(cmd.getOptionValue("c").toLowerCase());
        Class clazz = Class.forName(className);
        Runnable queryInstance = (Runnable) clazz.getDeclaredConstructor().newInstance();
        queryInstance.accept(cmd, args);
        if (cmd.getOptionValue("c").equalsIgnoreCase("flashdata")) {
            try {
                DistributeDataset.main(args);
            } catch (InterruptedException | IOException | SQLException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            return;
        } else if (cmd.getOptionValue("c").equalsIgnoreCase("LdbcStatistics")) {
            try {
                LDBCStatistics.main(args);
            } catch (InterruptedException | SQLException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            return;
        }
        Runnable runnable = null;
        switch (cmd.getOptionValue("c").toLowerCase()) {
            case "asyncpg2ugtest":
                runnable = new PG2UGTest();
                break;
            case "asyncrm2ugtest":
                runnable = new RM2UGTest();
                break;
            case "q1":
                runnable = new Q1();
                break;
            case "q2":
                runnable = new Q2();
                break;
            case "q3":
                runnable = new Q3();
                break;
            case "q4":
                runnable = new Q4();
                break;
            case "q5":
                runnable = new Q5();
                break;
            case "q6":
                runnable = new Q6();
                break;
            case "q7":
                runnable = new Q7();
                break;
        }
        if (runnable == null) {
            throw new RuntimeException("illegal parameters - c CLASS NAME: " + cmd.getOptionValue("c"));
        }
        runnable.accept(cmd, args);
    }
}
