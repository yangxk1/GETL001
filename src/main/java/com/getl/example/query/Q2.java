package com.getl.example.query;

import com.getl.api.GraphAPI;
import com.getl.constant.CommonConstant;
import com.getl.constant.IRINamespace;
import com.getl.converter.LPGGraphConverter;
import com.getl.converter.RMConverter;
import com.getl.converter.TinkerPopConverter;
import com.getl.example.Runnable;
import com.getl.example.utils.LoadUtil;
import com.getl.io.LPGParser;
import com.getl.model.RM.*;
import com.getl.model.ug.UnifiedGraph;
import com.getl.model.LPG.LPGEdge;
import com.getl.model.LPG.LPGGraph;
import com.getl.model.LPG.LPGVertex;
import com.getl.query.step.MultiLabelP;
import com.getl.util.DebugUtil;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.process.traversal.P.between;
import static org.apache.tinkerpop.gremlin.structure.T.label;

public class Q2 extends Runnable {
    public static void main(String[] args) throws ParseException, SQLException, ClassNotFoundException, InterruptedException {
        UnifiedGraph unifiedGraph = LoadUtil.loadUGFromPGFiles();
        long begin = System.currentTimeMillis();
        //GC
        Runtime.getRuntime().gc();
        DebugUtil.DebugInfo("GC " + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        GraphAPI graphAPI = GraphAPI.open();
        graphAPI.setUGMGraph(unifiedGraph);
        graphAPI.getDefaultConfig().addEdgeNamespaceList(IRINamespace.EDGE_NAMESPACE);
        graphAPI.refreshLPG();
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
        unifiedGraph = (new LPGGraphConverter(null, resultGraph, new HashMap<>())).createUGMFromLPGGraph();
        DebugUtil.DebugInfo("lpg result 2 ugm end " + (System.currentTimeMillis() - begin));
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
        rmConverter.addUGMToRMModel();
        DebugUtil.DebugInfo("ugm 2 RM end " + (System.currentTimeMillis() - begin));
        begin = System.currentTimeMillis();
        MysqlSessions sessions = new MysqlSessions(CommonConstant.RESULT_JDBC_URL_2, CommonConstant.JDBC_USERNAME, CommonConstant.JDBC_PASSWORD);
        MysqlOp.createSchema(sessions);
        MysqlOp.write(sessions, rmGraph);
        DebugUtil.DebugInfo("Write RM end " + (System.currentTimeMillis() - begin));
        System.out.println("Lines: " + rmGraph.getLines().size());

    }

    @Override
    public String init() {
        return validateParams(CommonConstant.LPG_FILES_BASE_URL);
    }

    @Override
    public void forward() {
        try {
            main(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
