package com.getl.constant;

import com.getl.model.ug.IRI;
import com.getl.model.ug.NamespacePool;

public class IRINamespace {
    public static IRI BLANK_NODE;
    public static final String STATEMENT_NAMESPACE = "http://getl.example.org/data/statement/";
    public static final String IRI_NAMESPACE = "http://getl.example.org/IRI/id/";
    public static final String LABEL_NAMESPACE = "http://getl.example.org/namespace/label/";
    public static final String PROPERTIES_NAMESPACE = "http://getl.example.org/propertes/";
    public static final String EDGE_NAMESPACE = "http://getl.example.org/edge/";

    public static String STATEMENT_NAMESPACE_ID;
    public static String IRI_NAMESPACE_ID;
    public static String LABEL_NAMESPACE_ID;
    public static String PROPERTIES_NAMESPACE_ID;
    public static String EDGE_NAMESPACE_ID;
   static {
        NamespacePool namespacePool = NamespacePool.getInstance();
        IRINamespace.STATEMENT_NAMESPACE_ID = namespacePool.getNamespaceId(STATEMENT_NAMESPACE);
        IRINamespace.IRI_NAMESPACE_ID = namespacePool.getNamespaceId(IRI_NAMESPACE);
        IRINamespace.LABEL_NAMESPACE_ID = namespacePool.getNamespaceId(LABEL_NAMESPACE);
        IRINamespace.PROPERTIES_NAMESPACE_ID = namespacePool.getNamespaceId(PROPERTIES_NAMESPACE);
        IRINamespace.EDGE_NAMESPACE_ID = namespacePool.getNamespaceId(EDGE_NAMESPACE);
        BLANK_NODE = new IRI(namespacePool.getNamespaceId("http://getl.example.org/namespace/"), "blankNode");
    }
}
