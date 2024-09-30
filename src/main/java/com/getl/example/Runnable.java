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

        forward();
    }
}
