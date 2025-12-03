package com.getl.converter;

import com.getl.api.GraphAPI;
import com.getl.constant.IRINamespace;
import com.getl.converter.SG.LPGMapper;
import com.getl.converter.SG.LPGMappingConfiguration;
import com.getl.converter.mg.PGMapperI;
import com.getl.converter.mg.PGMapperR4j;
import com.getl.converter.mg.RDFMapper;
import com.getl.converter.mg.RMMGConverter;
import com.getl.model.MG.MGraph;
import com.getl.model.RM.RMGraph;
import com.getl.model.RM.Schema;
import com.getl.model.ug.UnifiedGraph;
import com.getl.model.onegraph.dataset.OGDataset;
import com.getl.model.onegraph.LPG.LPGGraph;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.eclipse.rdf4j.model.Model;

import java.util.Map;

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

    public static RMGraph buildRMFromUGGraph(UnifiedGraph unifiedGraph, Map<String, Schema> schemas) {
        RMGraph rmGraph = new RMGraph();
        rmGraph.setSchemas(schemas);
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

    public static RMGraph buildRMFromMG(MGraph mGraph, Map<String, Schema> schemas) {
        RMGraph rmGraph = new RMGraph();
        rmGraph.setSchemas(schemas);
        RMMGConverter converter = new RMMGConverter(rmGraph, mGraph);
        converter.addMGToRM();
        return rmGraph;
    }


    public static Graph buildTinkerPopGraphFromSG(OGDataset ogDataset) {
        com.getl.converter.SG.TinkerPopConverter.TinkerPopConverterDelegate anonDelegate = new com.getl.converter.SG.TinkerPopConverter.TinkerPopConverterDelegate() {
            @Override
            public void unsupportedValueFound(String infoString) {
            }

            @Override
            public void metaPropertyFound(String infoString) {
            }

            @Override
            public void multiLabeledVertexFound(String infoString) {
            }
        };
        LPGMapper mapper = new LPGMapper(ogDataset, LPGMappingConfiguration.defaultConfiguration());
        LPGGraph lpgGraph = mapper.createLPGFromOGDataset();
        return (new com.getl.converter.SG.TinkerPopConverter(anonDelegate)).convertLPGToTinkerGraph(lpgGraph);
    }

    public static OGDataset buildSGFromTinkerPopGraph(Graph graph) {
        com.getl.converter.SG.TinkerPopConverter.TinkerPopConverterDelegate anonDelegate = new com.getl.converter.SG.TinkerPopConverter.TinkerPopConverterDelegate() {
            @Override
            public void unsupportedValueFound(String infoString) {
            }

            @Override
            public void metaPropertyFound(String infoString) {
            }

            @Override
            public void multiLabeledVertexFound(String infoString) {
            }
        };
        LPGGraph lpgGraph = (new com.getl.converter.SG.TinkerPopConverter(anonDelegate)).convertTinkerGraphToLPG(graph);
        LPGMapper mapper = new LPGMapper(LPGMappingConfiguration.defaultConfiguration());
        mapper.addLPGToOGDataset(lpgGraph);
        return mapper.dataset;
    }

    public static Model buildRDFGraphFromSG(OGDataset ogDataset) {
        com.getl.converter.SG.RDFMapper rdfMapper = new com.getl.converter.SG.RDFMapper(ogDataset);
        return rdfMapper.createRDFModelFromOGDataset();
    }

    public static OGDataset buildSGFromRDF(Model rdfGraph) {
        com.getl.converter.SG.RDFMapper rdfMapper = new com.getl.converter.SG.RDFMapper();
        rdfMapper.addRDFModelToOG(rdfGraph);
        return rdfMapper.dataset;
    }
}
