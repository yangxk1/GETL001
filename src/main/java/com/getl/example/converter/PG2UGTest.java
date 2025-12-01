package com.getl.example.converter;

import com.getl.constant.IRINamespace;
import com.getl.converter.LPGGraphConverter;
import com.getl.example.Runnable;
import com.getl.example.utils.LoadUtil;
import com.getl.model.LPG.LPGGraph;
import com.getl.model.ug.BasePair;
import com.getl.model.ug.IRI;
import com.getl.model.ug.NestedPair;
import com.getl.model.ug.UnifiedGraph;
import com.getl.util.GetlLogger;

import java.util.HashMap;
import java.util.stream.Collectors;

public class PG2UGTest extends Runnable {

    public static void main(String[] args) {
        new PG2UGTest().accept();
    }

    @Override
    protected void run() {
        try {
            UnifiedGraph unifiedGraph = LoadUtil.loadUGFromPGFiles(logger);
            System.out.println("ugm cache count: " + unifiedGraph.getCache().size());
            System.out.println("ugm edge count: " + unifiedGraph.getCache().stream().map(NestedPair::from).filter(basePair -> basePair.getLabels().stream().findFirst().map(IRI::getNameSpaceId).orElse("").equals(IRINamespace.EDGE_NAMESPACE_ID)).map(BasePair::getValueIRI).map(IRI::getLocalName).collect(Collectors.toSet()).size());
            System.out.println("ugm vertices count: " + unifiedGraph.traversal().V().count().next());
            Runtime.getRuntime().gc();
            long begin = System.currentTimeMillis();
            LPGGraph lpgGraphByUGM = new LPGGraphConverter(unifiedGraph, null, new HashMap<>()).createLPGGraphByUGM();
            logger.debugInfo("ugm 2 PG ", System.currentTimeMillis() - begin);
            System.out.println(lpgGraphByUGM.getVertices().size());
            System.out.println(lpgGraphByUGM.getEdges().size());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected GetlLogger initLogger() {
        return new GetlLogger("PG2UGTest");
    }
}
