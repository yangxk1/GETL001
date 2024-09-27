package com.getl.example;

import com.getl.example.async.AsyncPG2UGTest;
import com.getl.example.async.AsyncRM2UGTest;
import com.getl.example.query.*;
import com.getl.example.utils.FlashData;
import com.getl.example.utils.LdbcStatistics;
import org.apache.commons.cli.*;
import org.apache.commons.lang.StringUtils;

import java.io.IOException;
import java.sql.SQLException;

public class GetlExampleMain {

    public static void main(String[] args) throws ParseException {
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
        if (cmd.getOptionValue("c").equalsIgnoreCase("flashdata")) {
            try {
                FlashData.main(args);
            } catch (InterruptedException | IOException | SQLException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            return;
        } else if (cmd.getOptionValue("c").equalsIgnoreCase("LdbcStatistics")) {
            try {
                LdbcStatistics.main(args);
            } catch (InterruptedException | SQLException | ClassNotFoundException e) {
                throw new RuntimeException(e);
            }
            return;
        }
        Runnable runnable = null;
        switch (cmd.getOptionValue("c").toLowerCase()) {
            case "asyncpg2ugtest":
                runnable = new AsyncPG2UGTest();
                break;
            case "asyncrm2ugtest":
                runnable = new AsyncRM2UGTest();
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
