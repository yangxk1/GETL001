package com.getl.example;

import com.getl.constant.CommonConstant;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;

public abstract class Runnable {
    public abstract String init();

    public abstract void forward();

    public void accept(Options options, String[] args) {
        options.addOption("r", false, "RDF FILE URL");
        options.addOption("j", false, "JDBC URL");
        options.addOption("ju", false, "JDBC USER NAME");
        options.addOption("jp", false, "JDBC PASSWORD");
        options.addOption("p", false, "PROPERTY GRAPH FILE URL");
        DefaultParser parser = new DefaultParser();
        try {
            CommandLine commandLine = parser.parse(options, args);
            if (commandLine.hasOption("r")) {
                CommonConstant.RDF_FILES_BASE_URL = commandLine.getOptionValue("r");
            }
            if (commandLine.hasOption("j")) {
                CommonConstant.JDBC_URL = commandLine.getOptionValue("j");
            }
            if (commandLine.hasOption("ju")) {
                CommonConstant.JDBC_USERNAME = commandLine.getOptionValue("ju");
            }
            if (commandLine.hasOption("jp")) {
                CommonConstant.JDBC_PASSWORD = commandLine.getOptionValue("jp");
            }
            if (commandLine.hasOption("p")) {
                CommonConstant.LPG_FILES_BASE_URL = commandLine.getOptionValue("p");
            }
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
        String init = init();
        if (StringUtils.isNotBlank(init)) {
            throw new RuntimeException("Required parameters :" + init);
        }
        forward();
    }
}
