package com.getl.example.query;

import com.getl.converter.RMConverter;
import com.getl.example.utils.LoadUtil;
import com.getl.model.RM.Line;
import com.getl.model.RM.RMGraph;
import com.getl.model.ug.UnifiedGraph;

import java.sql.SQLException;
import java.util.Comparator;

public class TestTrans {
    public static void main(String[] args) throws SQLException, ClassNotFoundException, InterruptedException {
        UnifiedGraph unifiedGraph = LoadUtil.loadUGFromRMDataset();
        RMGraph rmGraph = new RMGraph();
        rmGraph.setSchemas(LoadUtil.schema);
        RMConverter rmConverter = new RMConverter(unifiedGraph, rmGraph);
        rmConverter.addUGMToRMModel();
        System.out.println("result rows count:"+rmConverter.rmGraph.getLines().size());
        rmConverter.rmGraph.getLines().values().stream().sorted(Comparator.comparing(Line::getId)).forEach(System.out::println);
    }
}
