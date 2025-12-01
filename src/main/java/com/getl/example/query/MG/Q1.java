package com.getl.example.query.MG;

import org.eclipse.rdf4j.model.Model;

/**
 * MG 对照实验 Q1: Model -> MG -> (简单统计) -> RDF Model
 */
public class Q1 extends AbstractMGQuery {
    public static void main(String[] args) {
        new Q1().accept();
    }

    @Override
    protected void run() {
        try {
            logger.debugInfo("BEGIN MG Q1");
            loadSourcePG();
            convertPGToMG();
            // 此处 MG 查询仅做规模统计
            logger.debugInfo("MG statement count: " + mGraph.size());
            Model rdf = exportMGToRDF();
            System.out.println("RDF SIZE : " + rdf.size());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected String getLoggerName() {
        return "MG_QUERY_1";
    }
}
