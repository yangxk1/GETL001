package com.getl.example.utils;

import com.getl.api.GraphAPI;
import com.getl.constant.CommonConstant;
import com.getl.converter.RMConverter;
import com.getl.converter.TinkerPopConverter;
import com.getl.io.LPGParser;
import com.getl.model.RM.MysqlOp;
import com.getl.model.RM.MysqlSessions;
import com.getl.model.RM.RMGraph;
import com.getl.model.RM.Schema;
import com.getl.model.ug.UnifiedGraph;
import com.getl.util.DebugUtil;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;

import java.io.*;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.eclipse.rdf4j.rio.helpers.BasicParserSettings.PRESERVE_BNODE_IDS;

public class FlashData {
    public static void main(String[] args) throws InterruptedException, IOException, SQLException, ClassNotFoundException {
//        Convert2PG();
        Convert2RDF();
        Convert2RM();
    }

    public static void statistics(String[] args) {
        String BASE_URL_STATIC = CommonConstant.LPG_FILES_BASE_URL + "static/";
        String BASE_URL_DYNAMIC = CommonConstant.LPG_FILES_BASE_URL + "dynamic/";
        loadCsv(BASE_URL_STATIC + "organisation_0_0.csv");
        loadCsv(BASE_URL_STATIC + "place_0_0.csv");
        loadCsv(BASE_URL_STATIC + "tag_0_0.csv");
        loadCsv(BASE_URL_STATIC + "tagclass_0_0.csv");
        loadCsv(BASE_URL_STATIC + "place_0_0.csv");
        //DYNAMIC
        loadCsv(BASE_URL_DYNAMIC + "comment_0_0.csv");
        loadCsv(BASE_URL_DYNAMIC + "forum_0_0.csv");
        loadCsv(BASE_URL_DYNAMIC + "person_0_0.csv");
        loadCsv(BASE_URL_DYNAMIC + "post_0_0.csv");
        loadCsv(BASE_URL_DYNAMIC + "person_email_emailaddress_0_0.csv");
        loadCsv(BASE_URL_DYNAMIC + "person_speaks_language_0_0.csv");

        System.out.println("============================ edges ====================================");
        //EDGE
        loadCsv(BASE_URL_STATIC + "organisation_isLocatedIn_place_0_0.csv");
        loadCsv(BASE_URL_STATIC + "place_isPartOf_place_0_0.csv");
        loadCsv(BASE_URL_STATIC + "tag_hasType_tagclass_0_0.csv");
        loadCsv(BASE_URL_STATIC + "tagclass_isSubclassOf_tagclass_0_0.csv");
        //DYNAMIC
        loadCsv(BASE_URL_DYNAMIC + "comment_hasCreator_person_0_0.csv");
        loadCsv(BASE_URL_DYNAMIC + "comment_hasTag_tag_0_0.csv");
        loadCsv(BASE_URL_DYNAMIC + "comment_isLocatedIn_place_0_0.csv");
        loadCsv(BASE_URL_DYNAMIC + "comment_replyOf_comment_0_0.csv");
        loadCsv(BASE_URL_DYNAMIC + "comment_replyOf_post_0_0.csv");
        loadCsv(BASE_URL_DYNAMIC + "forum_containerOf_post_0_0.csv");
        loadCsv(BASE_URL_DYNAMIC + "forum_hasMember_person_0_0.csv");
        loadCsv(BASE_URL_DYNAMIC + "forum_hasModerator_person_0_0.csv");
        loadCsv(BASE_URL_DYNAMIC + "forum_hasTag_tag_0_0.csv");
        loadCsv(BASE_URL_DYNAMIC + "person_hasInterest_tag_0_0.csv");
        loadCsv(BASE_URL_DYNAMIC + "person_isLocatedIn_place_0_0.csv");
        loadCsv(BASE_URL_DYNAMIC + "person_knows_person_0_0.csv");
        loadCsv(BASE_URL_DYNAMIC + "person_likes_comment_0_0.csv");
        loadCsv(BASE_URL_DYNAMIC + "person_likes_post_0_0.csv");
        loadCsv(BASE_URL_DYNAMIC + "person_studyAt_organisation_0_0.csv");
        loadCsv(BASE_URL_DYNAMIC + "person_workAt_organisation_0_0.csv");
        loadCsv(BASE_URL_DYNAMIC + "post_hasCreator_person_0_0.csv");
        loadCsv(BASE_URL_DYNAMIC + "post_hasTag_tag_0_0.csv");
        loadCsv(BASE_URL_DYNAMIC + "post_isLocatedIn_place_0_0.csv");

    }

    private static void loadCsv(String fileName) {
        try (Reader vertexReader = new FileReader(fileName)) {
            Iterable<CSVRecord> records = CSVFormat.INFORMIX_UNLOAD.withFirstRecordAsHeader().parse(vertexReader);
            Map<String, String> pop = new HashMap<>();
            for (CSVRecord record : records) {
                pop = record.toMap();
                break;
            }
            System.out.println(fileName.substring(CommonConstant.LPG_FILES_BASE_URL.length(), fileName.length() - 4) + ", " + String.join(", ", pop.keySet()));
            System.out.println("example" + ", " + String.join(", ", pop.values()));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static void Convert2RDF() throws InterruptedException, IOException {
        DebugUtil.DebugInfo("" + System.currentTimeMillis());
        String BASE_URL = CommonConstant.LPG_FILES_BASE_URL;
        LPGParser lpgParser = new LPGParser(new TinkerPopConverter(new UnifiedGraph(), null));
        long begin = System.currentTimeMillis();
        String BASE_URL_STATIC = BASE_URL + "static/";
        String BASE_URL_DYNAMIC = BASE_URL + "dynamic/";
        lpgParser.latchSize(6);
        lpgParser.loadVertex(BASE_URL_STATIC + "place_0_0.csv", "Place").commit2Converter();
        lpgParser.loadVertex(BASE_URL_STATIC + "tag_0_0.csv", "Tag").commit2Converter();
        lpgParser.loadVertex(BASE_URL_STATIC + "tagclass_0_0.csv", "TagClass").commit2Converter();
        lpgParser.loadVertexWithPro(BASE_URL_DYNAMIC + "person_0_0.csv", "Person", "locationIP", LPGParser.STRING, "browserUsed", LPGParser.STRING);
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "person_email_emailaddress_0_0.csv", "Person");
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "person_speaks_language_0_0.csv", "Person").commit2Converter();
        lpgParser.loadVertexWithPro(BASE_URL_DYNAMIC + "comment_0_0.csv", "Comment", "locationIP", LPGParser.STRING, "browserUsed", LPGParser.STRING).commit2Converter();
        lpgParser.loadVertexWithPro(BASE_URL_DYNAMIC + "post_0_0.csv", "Post", "locationIP", LPGParser.STRING, "browserUsed", LPGParser.STRING).commit2Converter();
        lpgParser.waitAll();
        //EDGE
        lpgParser.latchSize(3);
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "post_hasCreator_person_0_0.csv", "post_hasCreator_person", "Post", "Person").commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "post_hasTag_tag_0_0.csv", "post_hasTag_tag", "Post", "Tag").commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "post_isLocatedIn_place_0_0.csv", "post_isLocatedIn_place", "Post", "Place").commit2Converter();
        DebugUtil.DebugInfo("load pg end " + (System.currentTimeMillis() - begin));
        lpgParser.waitAll();
        DebugUtil.DebugInfo("commit pg end " + (System.currentTimeMillis() - begin));
        lpgParser.getAsyncPG2UMG().shutdown();
        System.out.println("pg vertices count:" + lpgParser.getGraph().traversal().V().count().next());
        System.out.println("pg edges count:" + lpgParser.getGraph().traversal().E().count().next());
        DebugUtil.DebugInfo("convert to ugm end " + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        UnifiedGraph unifiedGraph = lpgParser.getAsyncPG2UMG().getUnifiedGraph();
        lpgParser.setGraph(null);
        lpgParser.setAsyncPG2UMG(null);
        lpgParser = null;
        Runtime.getRuntime().gc();
        DebugUtil.DebugInfo("gc: " + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        GraphAPI graphAPI = GraphAPI.open(unifiedGraph);
        try {
            graphAPI.refreshRDF();
        } catch (OutOfMemoryError e) {
            DebugUtil.DebugInfo("oom");
            throw new RuntimeException(e);
        }
        DebugUtil.DebugInfo("convert to rdf end: " + (System.currentTimeMillis() - begin));
        System.out.println(graphAPI.getRDF().size());
        BufferedWriter fileWriter = new BufferedWriter(new FileWriter(CommonConstant.LDBC_RDF_FILES_URL));
        RDFWriter writer = Rio.createWriter(RDFFormat.NTRIPLES, fileWriter).set(PRESERVE_BNODE_IDS, true);
        writer.startRDF();
        for (Statement st : graphAPI.getRDF()) {
            writer.handleStatement(st);
        }
        writer.endRDF();
        System.out.println("write end: " + (System.currentTimeMillis() - begin));
    }

    private static void Convert2RM() throws InterruptedException, IOException, SQLException, ClassNotFoundException {
        DebugUtil.DebugInfo("" + System.currentTimeMillis());
        String BASE_URL = CommonConstant.LPG_FILES_BASE_URL;
        LPGParser lpgParser = new LPGParser(new TinkerPopConverter(new UnifiedGraph(), null));
        long begin = System.currentTimeMillis();
        String BASE_URL_STATIC = BASE_URL + "static/";
        String BASE_URL_DYNAMIC = BASE_URL + "dynamic/";
        lpgParser.latchSize(5);
        lpgParser.loadVertex(BASE_URL_STATIC + "organisation_0_0.csv", "Organisation").commit2Converter();
        lpgParser.loadVertexWithPro(BASE_URL_DYNAMIC + "person_0_0.csv", "Person", "firstName", LPGParser.STRING, "lastName", LPGParser.STRING, "gender", LPGParser.STRING, "birthday", LPGParser.DATE, "creationDate", LPGParser.DATE).commit2Converter();
        lpgParser.loadVertexWithPro(BASE_URL_DYNAMIC + "comment_0_0.csv", "Comment", "creationDate", LPGParser.DATE, "content", LPGParser.STRING, "length", LPGParser.INT).commit2Converter();
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "forum_0_0.csv", "Forum", "creationDate", LPGParser.DATE).commit2Converter();
        lpgParser.loadVertexWithPro(BASE_URL_DYNAMIC + "post_0_0.csv", "Post", "imageFile", LPGParser.STRING, "creationDate", LPGParser.DATE, "language", LPGParser.STRING, "content", LPGParser.STRING, "length", LPGParser.INT).commit2Converter();
        DebugUtil.DebugInfo("load pg end " + (System.currentTimeMillis() - begin));
        lpgParser.waitAll();
        DebugUtil.DebugInfo("commit pg end " + (System.currentTimeMillis() - begin));
        lpgParser.getAsyncPG2UMG().shutdown();
        System.out.println("pg vertices count:" + lpgParser.getGraph().traversal().V().count().next());
        System.out.println("pg edges count:" + lpgParser.getGraph().traversal().E().count().next());
        DebugUtil.DebugInfo("convert to ugm end " + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        UnifiedGraph unifiedGraph = lpgParser.getAsyncPG2UMG().getUnifiedGraph();
        lpgParser.setGraph(null);
        lpgParser.setAsyncPG2UMG(null);
        lpgParser = null;
        Runtime.getRuntime().gc();
        DebugUtil.DebugInfo("gc: " + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        RMGraph rmGraph = new RMGraph();
        rmGraph.addSchema(new Schema("Forum").addColumn("title", Schema.MID_LARGE_TEXT).addColumn("creationDate", Schema.DATE));
        rmGraph.addSchema(new Schema("Post").addColumn("imageFile", Schema.MID_LARGE_TEXT).addColumn("creationDate", Schema.DATE)
                .addColumn("length", Schema.INT).addColumn("browserUsed", Schema.SMALL_TEXT).addColumn("content", Schema.VERY_HUGE_TEXT));
        rmGraph.addSchema(new Schema("Comment").addColumn("length", Schema.INT).addColumn("creationDate", Schema.DATE)
                .addColumn("content", Schema.VERY_HUGE_TEXT));
        rmGraph.addSchema(new Schema("Person").addColumn("birthday", Schema.DATE)
                .addColumn("firstName", Schema.SMALL_TEXT).addColumn("lastName", Schema.SMALL_TEXT).
                addColumn("gender", Schema.SMALL_TEXT).addColumn("creationDate", Schema.DATE));
        rmGraph.addSchema(new Schema("Organisation").addColumn("type", Schema.MID_TEXT)
                .addColumn("name", Schema.MID_TEXT).addColumn("url", Schema.LARGE_TEXT));
        RMConverter rmConverter = new RMConverter(unifiedGraph, rmGraph);
        rmConverter.addUGMToRMModel();
        System.out.println("convert 2 rm end: " + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        MysqlSessions sessions = new MysqlSessions(CommonConstant.LDBC_JDBC_URL, CommonConstant.JDBC_USERNAME, CommonConstant.JDBC_PASSWORD);
        MysqlOp.createSchema(sessions);
        MysqlOp.write(sessions, rmGraph);
        System.out.println("write rm end: " + (System.currentTimeMillis() - begin));
    }
    private static void Convert2PG() throws InterruptedException, IOException, SQLException, ClassNotFoundException {
        LPGParser lpgParser = new LPGParser(new TinkerPopConverter(new UnifiedGraph(), null));
        long begin = System.currentTimeMillis();
        String BASE_URL = CommonConstant.LPG_FILES_BASE_URL;
        String BASE_URL_STATIC = BASE_URL + "static/";
        String BASE_URL_DYNAMIC = BASE_URL + "dynamic/";
        //EDGE
        lpgParser.latchSize(7);
        lpgParser.loadEdge(BASE_URL_STATIC + "organisation_isLocatedIn_place_0_0.csv", "organisation_isLocatedIn_place", "Organisation", "Place").commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "forum_hasMember_person_0_0.csv", "forum_hasMember_person", "Forum", "Person", "joinDate", LPGParser.DATE).commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_knows_person_0_0.csv", "person_knows_person", "Person", "Person", "creationDate", LPGParser.DATE).commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_likes_comment_0_0.csv", "person_likes_comment", "Person", "Comment", "creationDate", LPGParser.DATE).commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_likes_post_0_0.csv", "person_likes_post", "Person", "Post", "creationDate", LPGParser.DATE).commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_studyAt_organisation_0_0.csv", "person_studyAt_organisation", "Person", "Organisation", "classYear", LPGParser.INT).commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_workAt_organisation_0_0.csv", "person_workAt_organisation", "Person", "Organisation", "workFrom", LPGParser.INT).commit2Converter();
        DebugUtil.DebugInfo("load pg end " + (System.currentTimeMillis() - begin));
        lpgParser.waitAll();
        DebugUtil.DebugInfo("commit pg end " + (System.currentTimeMillis() - begin));
        System.out.println("pg count:" + lpgParser.getGraph().traversal().V().count().next());
        System.out.println(lpgParser.getGraph().traversal().E().count().next());
        Set<Object> set1 = lpgParser.getGraph().traversal().E().id().toSet().stream().map(i -> i.toString()).collect(Collectors.toSet());
        System.out.println(set1.size());
        lpgParser.getAsyncPG2UMG().shutdown();
        DebugUtil.DebugInfo("convert to ugm end " + (System.currentTimeMillis() - begin));
    }
}
