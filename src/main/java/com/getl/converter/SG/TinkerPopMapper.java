package com.getl.converter.SG;

import com.getl.model.onegraph.dataset.OGDataset;
import lombok.NonNull;
import org.apache.tinkerpop.gremlin.structure.Graph;

/**
 * Performs direct conversions between TinkerPop {@link Graph}s and {@link OGDataset}s in both directions,
 * without going through the intermediate LPGGraph representation.
 *
 * This mapper provides more efficient conversion by directly translating between TinkerPop's
 * property graph model and OneGraph's dataset model.
 */
public class TinkerPopMapper {

    /**
     * The OneGraph dataset this mapper is currently modifying.
     */
    public final OGDataset dataset;

    /**
     * The mapping configuration
     */
    public final LPGMappingConfiguration configuration;

    /**
     * The delegate for handling conversion events
     */
    public final TinkerPopConverterDelegate delegate;

    /**
     * Delegate interface for handling conversion events
     */
    public interface TinkerPopConverterDelegate {
        /**
         * Called when an unsupported property value type is encountered
         * @param infoString Information about the unsupported property
         */
        void unsupportedValueFound(String infoString);

        /**
         * Called when a meta-property is encountered
         * @param infoString Information about the meta-property
         */
        void metaPropertyFound(String infoString);

        /**
         * Called when a multi-labeled vertex is encountered
         * @param infoString Information about the multi-labeled vertex
         */
        void multiLabeledVertexFound(String infoString);
    }

    /**
     * Create a mapper with an existing dataset, configuration, and delegate.
     * @param dataset An existing OneGraph dataset.
     * @param configuration The mapping configuration.
     * @param delegate The converter delegate for handling events.
     */
    public TinkerPopMapper(@NonNull OGDataset dataset,
                          @NonNull LPGMappingConfiguration configuration,
                          TinkerPopConverterDelegate delegate) {
        this.dataset = dataset;
        this.configuration = configuration;
        this.delegate = delegate;
    }

    /**
     * Create a mapper with existing configuration and delegate, initializing a new dataset.
     * @param configuration The mapping configuration.
     * @param delegate The converter delegate for handling events.
     */
    public TinkerPopMapper(@NonNull LPGMappingConfiguration configuration,
                          TinkerPopConverterDelegate delegate) {
        this.dataset = new OGDataset();
        this.configuration = configuration;
        this.delegate = delegate;
    }

    /**
     * Create a mapper with default configuration and no delegate.
     * @param dataset An existing OneGraph dataset.
     */
    public TinkerPopMapper(@NonNull OGDataset dataset) {
        this.dataset = dataset;
        this.configuration = LPGMappingConfiguration.defaultConfiguration();
        this.delegate = null;
    }

    /**
     * Create a mapper with default configuration, no delegate, and a new dataset.
     */
    public TinkerPopMapper() {
        this.dataset = new OGDataset();
        this.configuration = LPGMappingConfiguration.defaultConfiguration();
        this.delegate = null;
    }

    /**
     * Adds the given TinkerPop graph to the current OneGraph dataset.
     * Directly converts TinkerPop vertices and edges to OG statements without intermediate LPG representation.
     *
     * @param graph The TinkerPop graph to add.
     */
    public void addTinkerPopGraphToOGDataset(@NonNull Graph graph) {
        TinkerPopToOG converter = new TinkerPopToOG(dataset, configuration);
        converter.addTinkerPopToOGDataset(graph);
    }

    /**
     * Creates a TinkerPop graph from the current OneGraph dataset.
     * Directly converts OG statements to TinkerPop vertices and edges without intermediate LPG representation.
     *
     * @return The TinkerPop graph created from the dataset.
     */
    public Graph createTinkerPopGraphFromOGDataset() {
        OGToTinkerPop converter = new OGToTinkerPop(dataset, configuration);
        return converter.createLPGFromOGDataset();
    }
}

