package com.getl;

import com.getl.constant.RdfDataFormat;
import com.getl.converter.LPGGraphConverter;
import com.getl.converter.PropertiesGraphConfig;
import com.getl.converter.RDFConverter;
import com.getl.model.ug.UnifiedGraph;
import com.getl.model.LPG.LPGGraph;
import lombok.Getter;
import lombok.NonNull;
import lombok.Setter;
import org.apache.tinkerpop.gremlin.tinkergraph.structure.TinkerGraph;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.repository.Repository;
import org.eclipse.rdf4j.repository.RepositoryConnection;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.repository.sail.SailRepository;
import org.eclipse.rdf4j.sail.memory.MemoryStore;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Getter
@Setter
public class Graph {

    private UnifiedGraph unifiedGraph;

    private final Repository rdfRepository;

    private boolean refreshWhenGet = false;

    private Model rdfModel;

    private TinkerGraph tinkerPopGraph;

    @Setter
    private LPGGraph lpgGraph;
    @Setter
    private long queryTimeoutMillis = 15000;

    @Getter     
    @Setter
    private LPGGraphConverter lpgGraphConverter;
    private RDFConverter rdfConverter;

    public Graph() {
        this.rdfRepository = new SailRepository(new MemoryStore());
        try {
            this.rdfRepository.init();
        } catch (RepositoryException e) {
            throw new RuntimeException(String.valueOf(e));
        }
        this.unifiedGraph = new UnifiedGraph();
        this.lpgGraph = new LPGGraph();
        this.rdfModel = new LinkedHashModel();
        this.tinkerPopGraph = TinkerGraph.open();
        this.rdfConverter = new RDFConverter(this.unifiedGraph);
        Map<String, PropertiesGraphConfig> lpgConfigs = new HashMap<>();
        lpgGraphConverter = new LPGGraphConverter(this.unifiedGraph, this.lpgGraph, lpgConfigs);
    }

    public void setDefaultConfig(PropertiesGraphConfig defaultConfig) {
        lpgGraphConverter.setDefaultConfig(defaultConfig);
    }

    public PropertiesGraphConfig getDefaultConfig() {
        return lpgGraphConverter.getDefaultConfig();
    }

    public void setConfig(Map<String, PropertiesGraphConfig> lpgConfigs) {
        lpgGraphConverter.setLpgConfigs(lpgConfigs);
    }

    public void refreshRDF() {
        this.rdfConverter.unifiedGraph = this.unifiedGraph;
        this.rdfModel = this.rdfConverter.createRDFModelFromUG();
    }

    public Repository getRdfRepository() {
        if (this.refreshWhenGet) {
            try (RepositoryConnection con = this.rdfRepository.getConnection()) {
                con.clear();
                con.add(this.rdfModel);
            }
        }
        return this.rdfRepository;
    }

    public void readRDFFileStream(@NonNull RdfDataFormat rdfDataFormat, @NonNull FileInputStream fileInputStream) {
        this.rdfModel = rdfConverter.readRDF(rdfDataFormat, fileInputStream);
    }

    public void readRDFFile(@NonNull RdfDataFormat rdfDataFormat, @NonNull File file) throws FileNotFoundException {
        FileInputStream fileInputStream = new FileInputStream(file);
        this.rdfModel = rdfConverter.readRDF(rdfDataFormat, fileInputStream);
        try {
            fileInputStream.close();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void handleRDFModel() {
        rdfConverter.addRDFModelToUG(this.rdfModel);
    }

    public void handleRDFFile(@NonNull RdfDataFormat rdfDataFormat, @NonNull File file) throws FileNotFoundException {
        FileInputStream fileInputStream = new FileInputStream(file);
        this.rdfModel = rdfConverter.handleRDF(rdfDataFormat, fileInputStream);
    }

    public void refreshLPG() {
        this.lpgGraphConverter.setUnifiedGraph(this.unifiedGraph);
        this.lpgGraph = lpgGraphConverter.createLPGGraphByUGM();
    }

    public void labelPredicate(String url) {
        this.rdfConverter.labelPredicate(url);
    }

    public void setRdfModel(Model model) {
        this.rdfModel = model;
    }
}