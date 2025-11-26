package com.getl.constant;

import com.getl.model.ug.IRI;
import com.getl.model.ug.NamespacePool;

public class IRINamespace {
    public static IRI BLANK_NODE;
    public static String STATEMENT_NAMESPACE = "http://getl.example.org/data/statement/";
    public static String IRI_NAMESPACE = "http://getl.example.org/IRI/id/";
    public static String LABEL_NAMESPACE = "http://getl.example.org/namespace/label/";
    public static String PROPERTIES_NAMESPACE = "http://getl.example.org/propertes/";
    public static String EDGE_NAMESPACE = "http://getl.example.org/edge/";
    void init() {
        NamespacePool namespacePool = NamespacePool.getInstance();
        IRINamespace.STATEMENT_NAMESPACE = namespacePool.getNamespaceId(STATEMENT_NAMESPACE);
        IRINamespace.IRI_NAMESPACE = namespacePool.getNamespaceId(IRI_NAMESPACE);
        IRINamespace.LABEL_NAMESPACE = namespacePool.getNamespaceId(LABEL_NAMESPACE);
        IRINamespace.PROPERTIES_NAMESPACE = namespacePool.getNamespaceId(PROPERTIES_NAMESPACE);
        IRINamespace.EDGE_NAMESPACE = namespacePool.getNamespaceId(EDGE_NAMESPACE);
        BLANK_NODE = new IRI(namespacePool.getNamespaceId("http://getl.example.org/namespace/"), "blankNode");
    }
}
