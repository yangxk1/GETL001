package com.getl.example.query;

import com.getl.api.GraphAPI;
import com.getl.constant.CommonConstant;
import com.getl.converter.LPGGraphConverter;
import com.getl.converter.RMConverter;
import com.getl.converter.TinkerPopConverter;
import com.getl.io.LPGParser;
import com.getl.model.ug.UnifiedGraph;
import com.getl.model.LPG.LPGEdge;
import com.getl.model.LPG.LPGGraph;
import com.getl.model.LPG.LPGVertex;
import com.getl.model.RM.MysqlOp;
import com.getl.model.RM.MysqlSessions;
import com.getl.model.RM.RMGraph;
import com.getl.model.RM.Schema;
import com.getl.query.step.MultiLabelP;
import com.getl.util.DebugUtil;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.tinkerpop.gremlin.process.traversal.P.between;
import static org.apache.tinkerpop.gremlin.structure.T.label;

public class Q2 {
    public static void main(String[] args) throws ParseException, SQLException, ClassNotFoundException {
        DebugUtil.DebugInfo("BEGIN TO TEST Q2");
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
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "comment_0_0.csv", "Comment", "creationDate", LPGParser.MILLI);
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "forum_0_0.csv", "Forum", "creationDate", LPGParser.MILLI);
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "person_0_0.csv", "Person", "birthday", LPGParser.MILLI, "creationDate", LPGParser.MILLI);
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "post_0_0.csv", "Post", "creationDate", LPGParser.MILLI, "length", LPGParser.INT);
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "person_email_emailaddress_0_0.csv", "Person");
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "person_speaks_language_0_0.csv", "Person");

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

        DebugUtil.DebugInfo("load pg end " + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        UnifiedGraph unifiedGraph = (new TinkerPopConverter(null, lpgParser.getGraph())).createKVGraphFromTinkerPopGraph();
        DebugUtil.DebugInfo("PG2UGM end " + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        //GC
        lpgParser.setGraph(null);
        lpgParser = null;
        Runtime.getRuntime().gc();
        DebugUtil.DebugInfo("GC " + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        GraphAPI graphAPI = GraphAPI.open();
        graphAPI.setKvGraph(unifiedGraph);   //配置简单图的kvGraph
        graphAPI.getDefaultConfig().addEdgeNamespaceList("http://kvgraph.example.org/edge");    //将具有http://kvgraph.example.org/edge前缀的关系识别为边
        graphAPI.refreshLPG();  //根据kvGraph更新属性图
        LPGGraph lpgGraph = graphAPI.getGraph().getLpgGraph();
        graphAPI.setGraph(null);
        unifiedGraph = null;
        graphAPI = null;
        DebugUtil.DebugInfo("UGM2LPG end " + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        Runtime.getRuntime().gc();
        DebugUtil.DebugInfo("GC" + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        //查询给2010-05月份发表的帖子回复的用户和其评论
        List<Map<String, Object>> commentsAndPersons = lpgGraph.traversal().V()
                .has(label, MultiLabelP.of("Forum"))
                .as("forum")
                .where(__.out("forum_hasMember_person").count().is(P.gte(20)))
                .out("forum_containerOf_post")
                .as("post")
                .has("creationDate", between(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2010-01-01 00:00:00"), new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2010-06-01 00:00:00")))
                .in("comment_replyOf_post") // 获取评论该Post的Comment
                .as("comment")
                .out("comment_hasCreator_person") // 获取Comment的创建者
                .as("person")
                .select("person", "comment", "post", "forum").toList();
        DebugUtil.DebugInfo("query end " + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        // 遍历查询结果，将用户及评论添加到新图中
        LPGGraph resultGraph = new LPGGraph();
        for (Object result : commentsAndPersons) {
            Map<String, Vertex> vertexMap = (Map<String, Vertex>) result;
            Vertex person = vertexMap.get("person");
            Vertex comment = vertexMap.get("comment");
            Vertex forum = vertexMap.get("forum");
            Vertex post = vertexMap.get("post");
            LPGVertex forumV = resultGraph.getOrCreateVertex(forum.id(), forum.label(), forum.properties());
            LPGVertex postV = resultGraph.getOrCreateVertex(post.id(), post.label(), post.properties());
            new LPGEdge(resultGraph, forumV, postV, "forum_containerOf_post");
            LPGVertex personV = resultGraph.getOrCreateVertex(person.id(), person.label(), person.properties());
            LPGVertex commentV = resultGraph.getOrCreateVertex(comment.id(), comment.label(), comment.properties());
            new LPGEdge(resultGraph, commentV, postV, "comment_replyOf_post");
            new LPGEdge(resultGraph, personV, commentV, "comment_hasCreator_person");
        }
        DebugUtil.DebugInfo("collect result to lpg end " + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        lpgGraph = null;
        Runtime.getRuntime().gc();
        DebugUtil.DebugInfo("GC" + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        unifiedGraph = (new LPGGraphConverter(null, resultGraph, new HashMap<>())).createKVGraphFromLPGGraph();
        DebugUtil.DebugInfo("lpg result 2 KV end " + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        resultGraph = null;
        Runtime.getRuntime().gc();
        DebugUtil.DebugInfo("GC" + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        RMGraph rmGraph = new RMGraph();
        rmGraph.addSchema(new Schema("comment_hasCreator_person", "Comment", "Person"));
        rmGraph.addSchema(new Schema("forum_containerOf_post", "Forum", "Post"));
        rmGraph.addSchema(new Schema("comment_replyOf_post", "Comment", "Post"));
        rmGraph.addSchema(new Schema("Forum").addColumn("title", Schema.MID_LARGE_TEXT).addColumn("creationDate", Schema.DATE));
        rmGraph.addSchema(new Schema("Post").addColumn("imageFile", Schema.MID_LARGE_TEXT).addColumn("creationDate", Schema.DATE)
                .addColumn("length", Schema.INT).addColumn("locationIP", Schema.SMALL_TEXT).addColumn("browserUsed", Schema.SMALL_TEXT)
                .addColumn("browserUsed", Schema.SMALL_TEXT).addColumn("content", Schema.VERY_HUGE_TEXT));
        rmGraph.addSchema(new Schema("Comment").addColumn("browserUsed", Schema.SMALL_TEXT)
                .addColumn("length", Schema.INT).addColumn("locationIP", Schema.SMALL_TEXT).
                addColumn("creationDate", Schema.DATE).addColumn("content", Schema.VERY_HUGE_TEXT));
        rmGraph.addSchema(new Schema("Person").addColumn("birthday", Schema.DATE)
                .addColumn("firstName", Schema.SMALL_TEXT).addColumn("lastName", Schema.SMALL_TEXT).
                addColumn("gender", Schema.SMALL_TEXT).addColumn("browserUsed", Schema.SMALL_TEXT).
                addColumn("locationIP", Schema.SMALL_TEXT).addColumn("language", Schema.SMALL_TEXT).
                addColumn("creationDate", Schema.DATE).addColumn("email", Schema.MID_LARGE_TEXT));
        RMConverter rmConverter = new RMConverter(unifiedGraph, rmGraph);
        rmConverter.addKVGraphToRMModel();
        DebugUtil.DebugInfo("KV 2 RM end " + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        MysqlSessions sessions = new MysqlSessions(CommonConstant.JDBC_URL, CommonConstant.JDBC_USERNAME, CommonConstant.JDBC_PASSWORD);
        MysqlOp.createSchema(sessions);
        MysqlOp.write(sessions, rmGraph);
        DebugUtil.DebugInfo("Write RM end " + (System.currentTimeMillis() - begin));
        System.out.println("Lines: " + rmGraph.getLines().size());

    }
}
