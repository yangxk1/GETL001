package com.getl.example;

import com.getl.constant.CommonConstant;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang3.StringUtils;

import java.sql.SQLException;

public abstract class Runnable {

    public String validateParams(String... params) {
        StringBuilder stringBuilder = new StringBuilder();
        for (String param : params) {
            if (StringUtils.isBlank(param)) {
                stringBuilder.append(param);
            }
        }
        if (stringBuilder.length() > 0) {
            return stringBuilder.toString();
        }
        return "";
    }

    public abstract String init();

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
        String init = init();
        if (StringUtils.isNotBlank(init)) {
            throw new RuntimeException("Required parameters :" + init);
        }
        forward();
    }
}
