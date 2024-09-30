package com.getl.example;

import com.getl.constant.CommonConstant;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

public abstract class Runnable {

    public abstract void forward();

    public void accept(CommandLine commandLine, String[] args) {
        try {
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
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        forward();
    }
}
