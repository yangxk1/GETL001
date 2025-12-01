package com.getl.example.query.MG;

import com.getl.example.Runnable;
import com.getl.util.GetlLogger;
import com.getl.constant.CommonConstant;
import com.getl.converter.mg.PGMapperI;
import com.getl.converter.mg.PGMapperR4j;
import com.getl.converter.mg.RDFMapper;
import com.getl.io.LPGParser;
import com.getl.model.MG.MGraph;
import com.getl.model.LPG.LPGEdge;
import com.getl.model.LPG.LPGGraph;
import com.getl.model.LPG.LPGVertex;
import org.apache.tinkerpop.gremlin.structure.*;
import org.eclipse.rdf4j.model.Model;

import java.util.Iterator;

/**
 * Base class for MG query experiments. Implements the pipeline:
 * Source Model (PG files) -> MG -> (convert back to PG/TinkerGraph for Gremlin query) -> Result Model (MG/RDF)
 * Each concrete query reuses the loading and conversion utilities here.
 */
public abstract class AbstractMGQuery extends Runnable {

    protected LPGParser lpgParser;
    protected MGraph mGraph;
    protected Graph pgGraphForQuery; // TinkerGraph produced from MG for traversal

    /**
     * Load the LPG (PG) source data set from CSV files (same dataset used in UG queries).
     */
    protected void loadSourcePG() {
        long begin = System.currentTimeMillis();
        lpgParser = new LPGParser();
        String BASE_URL = CommonConstant.LPG_FILES_BASE_URL;
        String BASE_URL_STATIC = BASE_URL + "static/";
        String BASE_URL_DYNAMIC = BASE_URL + "dynamic/";
        // STATIC vertices
        lpgParser.loadVertex(BASE_URL_STATIC + "organisation_0_0.csv", "Organisation");
        lpgParser.loadVertex(BASE_URL_STATIC + "place_0_0.csv", "Place");
        lpgParser.loadVertex(BASE_URL_STATIC + "tag_0_0.csv", "Tag");
        lpgParser.loadVertex(BASE_URL_STATIC + "tagclass_0_0.csv", "TagClass");
        // DYNAMIC vertices
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "comment_0_0.csv", "Comment", "creationDate", LPGParser.MILLI);
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "forum_0_0.csv", "Forum", "creationDate", LPGParser.MILLI);
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "person_0_0.csv", "Person", "birthday", LPGParser.MILLI, "creationDate", LPGParser.MILLI);
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "post_0_0.csv", "Post", "creationDate", LPGParser.MILLI, "length", LPGParser.INT);
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "person_email_emailaddress_0_0.csv", "Person");
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "person_speaks_language_0_0.csv", "Person");
        // STATIC edges
        lpgParser.loadEdge(BASE_URL_STATIC + "organisation_isLocatedIn_place_0_0.csv", "organisation_isLocatedIn_place", "Organisation", "Place");
        lpgParser.loadEdge(BASE_URL_STATIC + "place_isPartOf_place_0_0.csv", "place_isPartOf_place", "Place", "Place");
        lpgParser.loadEdge(BASE_URL_STATIC + "tag_hasType_tagclass_0_0.csv", "tag_hasType_tagclass", "Tag", "TagClass");
        lpgParser.loadEdge(BASE_URL_STATIC + "tagclass_isSubclassOf_tagclass_0_0.csv", "tagclass_isSubclassOf_tagclass", "TagClass", "TagClass");
        // DYNAMIC edges
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "comment_hasCreator_person_0_0.csv", "comment_hasCreator_person", "Comment", "Person");
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "comment_hasTag_tag_0_0.csv", "comment_hasTag_tag", "Comment", "Tag");
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "comment_isLocatedIn_place_0_0.csv", "comment_isLocatedIn_place", "Comment", "Place");
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "comment_replyOf_comment_0_0.csv", "comment_replyOf_comment", "Comment", "Comment");
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "comment_replyOf_post_0_0.csv", "comment_replyOf_post", "Comment", "Post");
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "forum_containerOf_post_0_0.csv", "forum_containerOf_post", "Forum", "Post");
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "forum_hasMember_person_0_0.csv", "forum_hasMember_person", "Forum", "Person", "joinDate", LPGParser.MILLI);
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "forum_hasModerator_person_0_0.csv", "forum_hasModerator_person", "Forum", "Person");
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "forum_hasTag_tag_0_0.csv", "forum_hasTag_tag", "Forum", "Tag");
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_hasInterest_tag_0_0.csv", "person_hasInterest_tag", "Person", "Tag");
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_isLocatedIn_place_0_0.csv", "person_isLocatedIn_place", "Person", "Place");
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_knows_person_0_0.csv", "person_knows_person", "Person", "Person", "creationDate", LPGParser.MILLI);
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_likes_comment_0_0.csv", "person_likes_comment", "Person", "Comment", "creationDate", LPGParser.MILLI);
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_likes_post_0_0.csv", "person_likes_post", "Person", "Post", "creationDate", LPGParser.MILLI);
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_studyAt_organisation_0_0.csv", "person_studyAt_organisation", "Person", "Organisation", "classYear", LPGParser.INT);
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_workAt_organisation_0_0.csv", "person_workAt_organisation", "Person", "Organisation", "workFrom", LPGParser.INT);
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "post_hasCreator_person_0_0.csv", "post_hasCreator_person", "Post", "Person");
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "post_hasTag_tag_0_0.csv", "post_hasTag_tag", "Post", "Tag");
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "post_isLocatedIn_place_0_0.csv", "post_isLocatedIn_place", "Post", "Place");
        logger.debugInfo("Load PG dataset end", (System.currentTimeMillis() - begin));
    }

    /** Convert loaded PG graph to MG. */
    protected void convertPGToMG() {
        long begin = System.currentTimeMillis();
        mGraph = new MGraph();
        PGMapperI pgMapper = new PGMapperR4j(mGraph);
        pgMapper.addPGToMG(lpgParser.getGraph());
        logger.debugInfo("PG -> MG end", (System.currentTimeMillis() - begin));
    }

    /** Create a TinkerPop Graph from MG for Gremlin traversal. */
    protected void preparePGForQuery() {
        long begin = System.currentTimeMillis();
        PGMapperI pgMapper = new PGMapperR4j(mGraph);
        pgGraphForQuery = pgMapper.createGraphFromMG();
        logger.debugInfo("MG -> PG (for query) end", (System.currentTimeMillis() - begin));
    }

    /** Wrap a generic TinkerPop Graph into LPGGraph for APIs expecting LPGGraph. */
    protected LPGGraph wrapAsLPG(Graph graph) {
        long begin = System.currentTimeMillis();
        LPGGraph lpgGraph = new LPGGraph();
        Iterator<Vertex> vIt = graph.vertices();
        while (vIt.hasNext()) {
            Vertex v = vIt.next();
            LPGVertex nv = lpgGraph.getOrCreateVertex(v.id(), v.label(), v.properties());
            Iterator<Edge> eIt = v.edges(Direction.OUT);
            while (eIt.hasNext()) {
                Edge e = eIt.next();
                Vertex in = e.inVertex();
                LPGVertex inV = lpgGraph.getOrCreateVertex(in.id(), in.label(), in.properties());
                LPGEdge ne = new LPGEdge(lpgGraph, nv, inV, e.label());
                ne.setId(e.id());
            }
        }
        logger.debugInfo("Wrap PG -> LPGGraph end", (System.currentTimeMillis() - begin));
        return lpgGraph;
    }

    /** Convert MG to RDF model for final output. */
    protected Model exportMGToRDF() {
        long begin = System.currentTimeMillis();
        RDFMapper rdfMapper = new RDFMapper(mGraph);
        Model model = rdfMapper.createRDFModelFromMG();
        logger.debugInfo("MG -> RDF end", (System.currentTimeMillis() - begin));
        return model;
    }

    @Override
    protected GetlLogger initLogger() {
        return new GetlLogger(getLoggerName());
    }

    /** Each subclass supplies a concise logger name. */
    protected abstract String getLoggerName();
}
