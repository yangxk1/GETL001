package com.getl.experiment;

import com.getl.constant.CommonConstant;
import com.getl.example.Runnable;
import org.apache.commons.cli.*;
import org.yaml.snakeyaml.Yaml;

import java.io.InputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.HashMap;
import java.util.Map;

public class GetlExperimentMain {

    private static Map<String, String> classMap;

    public static String yamlGet(Map<String, Object> obj, String uri) {
        String[] split = uri.split("\\.");
        Map resultMap = obj;
        for (int i = 0; i < split.length - 1; i++) {
            resultMap = (Map) resultMap.get(split[i]);
        }
        return resultMap.get(split[split.length - 1]).toString();
    }

    public static void init() {
        Yaml yaml = new Yaml();
        InputStream inputStream = GetlExperimentMain.class.getClassLoader().getResourceAsStream("config.yaml");
        Map<String, Object> obj = yaml.load(inputStream);
        CommonConstant.JDBC_BASE_URL = yamlGet(obj, "jdbc.url");
        CommonConstant.JDBC_USERNAME = yamlGet(obj, "jdbc.username");
        CommonConstant.JDBC_PASSWORD = yamlGet(obj, "jdbc.password");
        CommonConstant.JDBC_URL = CommonConstant.JDBC_BASE_URL + yamlGet(obj, "jdbc.database.base");
        CommonConstant.RESULT_JDBC_URL_2 = CommonConstant.JDBC_BASE_URL + yamlGet(obj, "jdbc.database.q2");
        CommonConstant.RESULT_JDBC_URL_6 = CommonConstant.JDBC_BASE_URL + yamlGet(obj, "jdbc.database.q6");
        CommonConstant.LDBC_JDBC_URL = CommonConstant.JDBC_BASE_URL + yamlGet(obj, "jdbc.database.ldbc.source");
        CommonConstant.LDBC_JDBC_RESULT = CommonConstant.JDBC_BASE_URL + yamlGet(obj, "jdbc.database.ldbc.target");
        CommonConstant.LPG_FILES_BASE_URL = yamlGet(obj, "lpg.url.base");
        CommonConstant.RDF_FILES_BASE_URL = yamlGet(obj, "rdf.url.base.source");
        CommonConstant.RDF_FILES_BASE_URL = yamlGet(obj, "rdf.url.base.target");
        CommonConstant.LDBC_RDF_FILES_URL = yamlGet(obj, "rdf.url.ldbc");

        classMap = new HashMap<>();
        classMap.put("q1", "com.getl.experiment.query.Q1");
        classMap.put("q2", "com.getl.experiment.query.Q2");
        classMap.put("q3", "com.getl.experiment.query.Q3");
        classMap.put("q4", "com.getl.experiment.query.Q4");
        classMap.put("q5", "com.getl.experiment.query.Q5");
        classMap.put("q6", "com.getl.experiment.query.Q6");
        classMap.put("q7", "com.getl.experiment.query.Q7");
        classMap.put("lpg", "com.getl.experiment.RuntimeOfModelConversion.LPG");
        classMap.put("rdf", "com.getl.experiment.RuntimeOfModelConversion.RDF");
        classMap.put("rm", "com.getl.experiment.RuntimeOfModelConversion.RM");
    }

    public static void main(String[] args) throws ParseException, ClassNotFoundException, NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        init();
        Options options = new Options();
        options.addOption("c", true, "CLASS NAME");
        options.addOption("m", true, "MODEL NAME");
        CommandLineParser parser = new DefaultParser();
        parser.parse(options, args);
        CommandLine cmd = parser.parse(options, args);
        if (!cmd.hasOption("c")) {
            StringBuilder stringBuilder = new StringBuilder();
            classMap.forEach((k, v) -> {
                stringBuilder.append("       ").append(k).append(" : ").append(v).append("\n");
            });
            throw new RuntimeException("Required parameters -c CLASS \n" + stringBuilder);
        }
        String className = classMap.get(cmd.getOptionValue("c").toLowerCase()) == null ? cmd.getOptionValue("c") : classMap.get(cmd.getOptionValue("c").toLowerCase());
        String modelName = cmd.hasOption("m") ? cmd.getOptionValue("m").toUpperCase() : "UG";
        Class clazz = Class.forName(className);
        com.getl.example.Runnable queryInstance = (Runnable) clazz.getDeclaredConstructor(String.class).newInstance(modelName);
        queryInstance.accept();
    }
}
