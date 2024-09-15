package com.getl.api;

import com.getl.Graph;
import com.getl.constant.RdfDataFormat;
import com.getl.converter.LPGSuperVertexBuilder;
import com.getl.converter.PropertiesGraphConfig;
import com.getl.model.ug.UnifiedGraph;
import lombok.Data;
import lombok.NonNull;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.repository.Repository;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Map;

@Data
public class GraphAPI {
    private Graph graph;

    public static GraphAPI open() {
        GraphAPI graphAPI = new GraphAPI();
        graphAPI.graph = new Graph();
        return graphAPI;
    }

    public static GraphAPI open(UnifiedGraph unifiedGraph) {
        Graph graph1 = new Graph();
        graph1.setUnifiedGraph(unifiedGraph);
        GraphAPI graphAPI = new GraphAPI();
        graphAPI.graph = graph1;
        return graphAPI;
    }

    public static GraphAPI open(Graph graph) {
        GraphAPI graphAPI = new GraphAPI();
        graphAPI.graph = graph;
        return graphAPI;
    }

    public GraphTraversalSource traversal() {
        return this.graph.getLpgGraph().traversal();
    }

    public void getDefaultConfig(PropertiesGraphConfig defaultConfig) {
        graph.setDefaultConfig(defaultConfig);
    }

    public PropertiesGraphConfig getDefaultConfig() {
        if (graph.getDefaultConfig() == null) {
            PropertiesGraphConfig propertiesGraphConfig = new PropertiesGraphConfig();
            graph.setDefaultConfig(propertiesGraphConfig);
        }
        return graph.getDefaultConfig();
    }

    public void setConfig(Map<String, PropertiesGraphConfig> lpgConfigs) {
        graph.setConfig(lpgConfigs);
    }

    public Model getRDF() {
        return graph.getRdfModel();
    }

    public void refreshRDF() {
        graph.refreshRDF();
    }

    public Repository getRdfRepository() {
        return graph.getRdfRepository();
    }

    public void readRDFFile(@NonNull RdfDataFormat rdfDataFormat, @NonNull File file) throws FileNotFoundException {
        graph.readRDFFile(rdfDataFormat, file);
    }

    public void refreshLPG() {
        graph.refreshLPG();
    }

    public LPGSuperVertexBuilder buildSuperVertex() {
        return LPGSuperVertexBuilder.getLPGSuperVertexBuilder(graph);
    }

    public void addUGMPairs(UnifiedGraph sourceUnifiedGraph) {
        UnifiedGraph targetUnifiedGraph = graph.getUnifiedGraph();
        targetUnifiedGraph.merge(sourceUnifiedGraph);
    }

    public void setUGMGraph(UnifiedGraph unifiedGraph) {
        this.graph.setUnifiedGraph(unifiedGraph);
    }
}
