package com.getl.experiment.query;

import com.getl.experiment.ExperimentRunner;
import org.apache.tinkerpop.gremlin.structure.Graph;

public abstract class QueryRunner extends ExperimentRunner {
    protected abstract Graph Transform(Graph graph);
}
