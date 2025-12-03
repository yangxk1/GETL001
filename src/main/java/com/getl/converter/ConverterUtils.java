package com.getl.converter;

import com.getl.api.GraphAPI;
import com.getl.constant.IRINamespace;
import com.getl.converter.mg.PGMapperI;
import com.getl.converter.mg.PGMapperR4j;
import com.getl.converter.mg.RDFMapper;
import com.getl.converter.mg.RMMGConverter;
import com.getl.model.MG.MGraph;
import com.getl.model.RM.RMGraph;
import com.getl.model.ug.UnifiedGraph;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.eclipse.rdf4j.model.Model;

public class ConverterUtils {
    public static Graph buildTinkerPopGraphFromUG(UnifiedGraph unifiedGraph) {
        GraphAPI graphAPI = GraphAPI.open();
        graphAPI.setUGMGraph(unifiedGraph);
        graphAPI.getDefaultConfig().addEdgeNamespaceList(IRINamespace.EDGE_NAMESPACE_ID);
        graphAPI.refreshLPG();
        return graphAPI.getGraph().getLpgGraph();
    }

    public static UnifiedGraph buildUGFromTinkerPopGraph(Graph tinkerPopGraph) {
        return (new TinkerPopConverter(null, tinkerPopGraph)).createUGMFromTinkerPopGraph();
    }

    public static Model buildRDFGraphFromUG(UnifiedGraph unifiedGraph) {
        RDFConverter rdfConverter = new RDFConverter(unifiedGraph);
        return rdfConverter.createRDFModelFromUG();
    }

    public static UnifiedGraph buildUGGraphFromRDF(Model rdfGraph) {
        RDFConverter rdfConverter = new RDFConverter();
        rdfConverter.addRDFModelToUG(rdfGraph);
        return rdfConverter.getUnifiedGraph();
    }

    public static RMGraph buildRMFromUGGraph(UnifiedGraph unifiedGraph) {
        RMGraph rmGraph = new RMGraph();
        RMConverter converter = new RMConverter(unifiedGraph, rmGraph);
        converter.addUGMToRMModel();
        return rmGraph;
    }

    public static UnifiedGraph buildUGGraphFromRM(RMGraph rmGraph) {
        UnifiedGraph unifiedGraph = new UnifiedGraph();
        RMConverter converter = new RMConverter(unifiedGraph, rmGraph);
        converter.addRMModelToUGM();
        return unifiedGraph;
    }

    public static Graph buildTinkerPopGraphFromMG(MGraph mGraph) {
        PGMapperI pgMapper = new PGMapperR4j(mGraph);
        return pgMapper.createGraphFromMG();
    }

    public static MGraph buildMGFromTinkerPopGraph(Graph tinkerPopGraph) {
        MGraph mGraph = new MGraph();
        PGMapperI pgMapper = new PGMapperR4j(mGraph);
        pgMapper.addPGToMG(tinkerPopGraph);
        return mGraph;
    }

    public static Model buildRDFGraphFromMG(MGraph mGraph) {
        RDFMapper RDFMapper = new RDFMapper(mGraph);
        return RDFMapper.createRDFModelFromMG();
    }

    public static MGraph buildMGGraphFromRDF(Model rdfGraph) {
        MGraph mGraph = new MGraph();
        RDFMapper RDFMapper = new RDFMapper(new MGraph());
        RDFMapper.addRDFModelToMG(rdfGraph);
        return mGraph;
    }

    public static MGraph buildMGFromRM(RMGraph rmGraph) {
        MGraph mGraph = new MGraph();
        RMMGConverter converter = new RMMGConverter(rmGraph, mGraph);
        converter.addRMToMG();
        return mGraph;
    }

    public static RMGraph buildRMFromMG(MGraph mGraph) {
        RMGraph rmGraph = new RMGraph();
        RMMGConverter converter = new RMMGConverter(rmGraph, mGraph);
        converter.addMGToRM();
        return rmGraph;
    }
}
