package com.getl.example;

import com.getl.example.async.AsyncPG2UGTest;
import com.getl.example.async.AsyncRM2UGTest;
import org.apache.commons.cli.*;

public class GetlExampleMain {

    public static void main(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("c", true, "CLASS NAME");
        CommandLineParser parser = new DefaultParser();
        parser.parse(options, args);
        CommandLine cmd = parser.parse(options, args);
        if (!cmd.hasOption("c")) {
            throw new RuntimeException("Required parameters -c CLASS NAME");
        }
        Runnable runnable = null;
        switch (cmd.getOptionValue("c")) {
            case "AsyncPG2UGTest":
                runnable = new AsyncPG2UGTest();
                break;
            case "AsyncRM2UGTest":
                runnable = new AsyncRM2UGTest();
                break;
        }
        if (runnable == null) {
            throw new RuntimeException("illegal parameters - c CLASS NAME");
        }
        runnable.accept(options, args);
    }
}
