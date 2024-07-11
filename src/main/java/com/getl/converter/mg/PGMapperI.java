package com.getl.converter.mg;

import lombok.NonNull;
import org.apache.tinkerpop.gremlin.structure.Graph;

public interface PGMapperI {
    public Graph createGraphFromMG();
    public void addPGToMG(@NonNull Graph graph);
}
