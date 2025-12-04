package com.getl.example.query.MG;

import com.getl.constant.CommonConstant;
import com.getl.converter.TinkerPopConverter;
import com.getl.converter.mg.PGMapperI;
import com.getl.converter.mg.PGMapperR4j;
import com.getl.converter.mg.RDFMapper;
import com.getl.example.Runnable;
import com.getl.io.LPGParser;
import com.getl.model.MG.MGraph;
import com.getl.model.ug.UnifiedGraph;
import com.getl.util.GetlLogger;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.eclipse.rdf4j.model.Model;

import static org.apache.tinkerpop.gremlin.process.traversal.P.eq;
import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.in;

public class Q4 extends Runnable {
    public static void main(String[] args) {
        new Q4().accept();
    }

    @Override
    protected void run() {
        try {
            logger.debugInfo("BEGIN TO TEST Q4");
            String BASE_URL = CommonConstant.LPG_FILES_BASE_URL;
            LPGParser lpgParser = new LPGParser();
            long begin = System.currentTimeMillis();
            String BASE_URL_STATIC = BASE_URL + "static/";
            String BASE_URL_DYNAMIC = BASE_URL + "dynamic/";
            lpgParser.loadVertex(BASE_URL_STATIC + "organisation_0_0.csv", "Organisation");
            lpgParser.loadVertex(BASE_URL_STATIC + "place_0_0.csv", "Place");
            lpgParser.loadVertex(BASE_URL_STATIC + "tag_0_0.csv", "Tag");
            lpgParser.loadVertex(BASE_URL_STATIC + "tagclass_0_0.csv", "TagClass");
            //DYNAMIC
            lpgParser.loadVertex(BASE_URL_DYNAMIC + "person_0_0.csv", "Person", "birthday", LPGParser.STRING, "creationDate", LPGParser.STRING);
            lpgParser.loadVertex(BASE_URL_DYNAMIC + "person_email_emailaddress_0_0.csv", "Person");
            lpgParser.loadVertex(BASE_URL_DYNAMIC + "person_speaks_language_0_0.csv", "Person");

            lpgParser.loadVertex(BASE_URL_DYNAMIC + "comment_0_0.csv", "Comment", "creationDate", LPGParser.STRING);
            lpgParser.loadVertex(BASE_URL_DYNAMIC + "forum_0_0.csv", "Forum", "creationDate", LPGParser.STRING);
            lpgParser.loadVertex(BASE_URL_DYNAMIC + "post_0_0.csv", "Post", "creationDate", LPGParser.STRING, "length", LPGParser.INT);
            //EDGE
            lpgParser.loadEdge(BASE_URL_STATIC + "organisation_isLocatedIn_place_0_0.csv", "organisation_isLocatedIn_place", "Organisation", "Place");
            lpgParser.loadEdge(BASE_URL_STATIC + "place_isPartOf_place_0_0.csv", "place_isPartOf_place", "Place", "Place");
            lpgParser.loadEdge(BASE_URL_STATIC + "tag_hasType_tagclass_0_0.csv", "tag_hasType_tagclass", "Tag", "TagClass");
            lpgParser.loadEdge(BASE_URL_STATIC + "tagclass_isSubclassOf_tagclass_0_0.csv", "tagclass_isSubclassOf_tagclass", "TagClass", "TagClass");
            //DYNAMIC
            lpgParser.loadEdge(BASE_URL_DYNAMIC + "comment_hasCreator_person_0_0.csv", "comment_hasCreator_person", "Comment", "Person");
            lpgParser.loadEdge(BASE_URL_DYNAMIC + "comment_hasTag_tag_0_0.csv", "comment_hasTag_tag", "Comment", "Tag");
            lpgParser.loadEdge(BASE_URL_DYNAMIC + "comment_isLocatedIn_place_0_0.csv", "comment_isLocatedIn_place", "Comment", "Place");
            lpgParser.loadEdge(BASE_URL_DYNAMIC + "comment_replyOf_comment_0_0.csv", "comment_replyOf_comment", "Comment", "Comment");
            lpgParser.loadEdge(BASE_URL_DYNAMIC + "comment_replyOf_post_0_0.csv", "comment_replyOf_post", "Comment", "Post");
            lpgParser.loadEdge(BASE_URL_DYNAMIC + "forum_containerOf_post_0_0.csv", "forum_containerOf_post", "Forum", "Post");
            lpgParser.loadEdge(BASE_URL_DYNAMIC + "forum_hasMember_person_0_0.csv", "forum_hasMember_person", "Forum", "Person", "joinDate", LPGParser.STRING);
            lpgParser.loadEdge(BASE_URL_DYNAMIC + "forum_hasModerator_person_0_0.csv", "forum_hasModerator_person", "Forum", "Person");
            lpgParser.loadEdge(BASE_URL_DYNAMIC + "forum_hasTag_tag_0_0.csv", "forum_hasTag_tag", "Forum", "Tag");
            lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_hasInterest_tag_0_0.csv", "person_hasInterest_tag", "Person", "Tag");
            lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_isLocatedIn_place_0_0.csv", "person_isLocatedIn_place", "Person", "Place");
            lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_knows_person_0_0.csv", "person_knows_person", "Person", "Person", "creationDate", LPGParser.STRING);
            lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_likes_comment_0_0.csv", "person_likes_comment", "Person", "Comment", "creationDate", LPGParser.STRING);
            lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_likes_post_0_0.csv", "person_likes_post", "Person", "Post", "creationDate", LPGParser.STRING);
            lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_studyAt_organisation_0_0.csv", "person_studyAt_organisation", "Person", "Organisation", "classYear", LPGParser.INT);
            lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_workAt_organisation_0_0.csv", "person_workAt_organisation", "Person", "Organisation", "workFrom", LPGParser.INT);
            lpgParser.loadEdge(BASE_URL_DYNAMIC + "post_hasCreator_person_0_0.csv", "post_hasCreator_person", "Post", "Person");
            lpgParser.loadEdge(BASE_URL_DYNAMIC + "post_hasTag_tag_0_0.csv", "post_hasTag_tag", "Post", "Tag");
            lpgParser.loadEdge(BASE_URL_DYNAMIC + "post_isLocatedIn_place_0_0.csv", "post_isLocatedIn_place", "Post", "Place");
            logger.debugInfo("load pg files end ", (System.currentTimeMillis() - begin));

            begin = System.currentTimeMillis();
            MGraph mGraph = new MGraph();
            PGMapperI pgMapper = new PGMapperR4j(mGraph);
            pgMapper.addPGToMG(lpgParser.getGraph());
            logger.debugInfo("PG2MG end ", (System.currentTimeMillis() - begin));

            //GC
            begin = System.currentTimeMillis();
            pgMapper = null;
            lpgParser = null;
            Runtime.getRuntime().gc();
            logger.debugInfo("GC ", (System.currentTimeMillis() - begin));

            begin = System.currentTimeMillis();
            pgMapper = new PGMapperR4j(mGraph);
            org.apache.tinkerpop.gremlin.structure.Graph lpgGraph = pgMapper.createGraphFromMG();
            logger.debugInfo("graph customization 2 LPG end ", System.currentTimeMillis() - begin);

            begin = System.currentTimeMillis();
            mGraph = null;
            pgMapper = null;
            Runtime.getRuntime().gc();
            logger.debugInfo("GC", (System.currentTimeMillis() - begin));
            begin = System.currentTimeMillis();
            lpgGraph.traversal().V().hasLabel("Person").as("person1")
                    .in("comment_hasCreator_person").as("comment")
                    .out("comment_replyOf_post").as("post")
                    .out("post_hasCreator_person").as("person2")
                    .where("person1", P.neq("person2"))
                    .not(in("person_isFanOf_person").where(eq("person1")))
                    .addE("person_isFanOf_person")
                    .from("person1")
                    .to("person2")
                    //  .select("person1", "comment", "post", "person2")
                    .toList();
            logger.debugInfo("query end ", (System.currentTimeMillis() - begin));

            begin = System.currentTimeMillis();
            mGraph = new MGraph();
            pgMapper = new PGMapperR4j(mGraph);
            pgMapper.addPGToMG(lpgGraph);
            logger.debugInfo("lpg result 2 MG end ", (System.currentTimeMillis() - begin));

            begin = System.currentTimeMillis();
            lpgGraph = null;
            pgMapper = null;
            Runtime.getRuntime().gc();
            logger.debugInfo("GC", (System.currentTimeMillis() - begin));

            begin = System.currentTimeMillis();
            RDFMapper RDFMapper = new RDFMapper(mGraph);
            Model rdfModelFromMG = RDFMapper.createRDFModelFromMG();
            logger.debugInfo("MG2RDF end " , (System.currentTimeMillis() - begin));
            System.out.println("RDF SIZE : " + rdfModelFromMG.size());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected String loggerName() {
        return "MG QUERY 4";
    }
}
