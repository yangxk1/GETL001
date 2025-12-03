package com.getl.converter;

import com.getl.api.GraphAPI;
import com.getl.constant.IRINamespace;
import com.getl.converter.mg.PGMapperI;
import com.getl.converter.mg.PGMapperR4j;
import com.getl.converter.mg.RDFMapper;
import com.getl.model.MG.MGraph;
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
}
