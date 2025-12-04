package com.getl.experiment.query;

import com.getl.converter.ConverterUtils;
import com.getl.experiment.DataLoadUtils;
import com.getl.model.LPG.LPGEdge;
import com.getl.model.LPG.LPGGraph;
import com.getl.model.LPG.LPGVertex;
import com.getl.model.MG.MGraph;
import com.getl.model.RM.RMGraph;
import com.getl.model.RM.Schema;
import com.getl.model.onegraph.dataset.OGDataset;
import com.getl.model.ug.UnifiedGraph;
import com.getl.query.step.MultiLabelP;
import com.getl.util.GetlLogger;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

public class Q2 extends QueryRunner {
    public Q2(String name) {
        super(name);
    }

    public static void main(String[] args) {
        new Q2("UG").accept();
    }

    private Graph graphData;

    private Map<String, Schema> buildRMSchema() {
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
        return rmGraph.getSchemas();
    }

    @Override
    protected void loadData() {
        this.graphData = DataLoadUtils.loadTinkerPopGraph();
    }

    @Override
    protected void UG() {
        long begin = System.currentTimeMillis();
        UnifiedGraph unifiedGraph = ConverterUtils.buildUGFromTinkerPopGraph(graphData);
        logger.debugInfo("transFromLPG-UG", System.currentTimeMillis() - begin);

        begin = System.currentTimeMillis();
        graphData = null;
        super.forceGC();
        logger.debugInfo("GC ", (System.currentTimeMillis() - begin));

        begin = System.currentTimeMillis();
        Graph graphView = ConverterUtils.buildTinkerPopGraphFromUG(unifiedGraph);
        logger.debugInfo("graph customization-UG", System.currentTimeMillis() - begin);

        begin = System.currentTimeMillis();
        Runtime.getRuntime().gc();
        logger.debugInfo("GC ", (System.currentTimeMillis() - begin));

        begin = System.currentTimeMillis();
        Graph result = Transform(graphView);
        logger.debugInfo("transformation-UG", System.currentTimeMillis() - begin);

        begin = System.currentTimeMillis();
        Runtime.getRuntime().gc();
        logger.debugInfo("GC ", (System.currentTimeMillis() - begin));

        begin = System.currentTimeMillis();
        UnifiedGraph ugResult = ConverterUtils.buildUGFromTinkerPopGraph(result);
        logger.debugInfo("ResultTransFromLPG-UG", System.currentTimeMillis() - begin);

        begin = System.currentTimeMillis();
        Runtime.getRuntime().gc();
        logger.debugInfo("GC ", (System.currentTimeMillis() - begin));

        begin = System.currentTimeMillis();
        RMGraph resultRM = ConverterUtils.buildRMFromUGGraph(ugResult, buildRMSchema());
        logger.debugInfo("transTORM-UG", System.currentTimeMillis() - begin);
        logger.info("Lines: " + resultRM.getLines().size());

//        MysqlSessions sessions = new MysqlSessions(CommonConstant.RESULT_JDBC_URL_2, CommonConstant.JDBC_USERNAME, CommonConstant.JDBC_PASSWORD);
//        MysqlOp.createSchema(sessions);
//        MysqlOp.write(sessions, rmGraph);
    }

    @Override
    protected void SG() {
        long begin = System.currentTimeMillis();
        OGDataset ogDataset = ConverterUtils.buildSGFromTinkerPopGraph(graphData);
        logger.debugInfo("transFromLPG-SG", System.currentTimeMillis() - begin);

        begin = System.currentTimeMillis();
        graphData = null;
        super.forceGC();
        logger.debugInfo("GC ", (System.currentTimeMillis() - begin));

        begin = System.currentTimeMillis();
        Graph graphView = ConverterUtils.buildTinkerPopGraphFromSG(ogDataset);
        logger.debugInfo("graph customization-SG", System.currentTimeMillis() - begin);

        begin = System.currentTimeMillis();
        Runtime.getRuntime().gc();
        logger.debugInfo("GC ", (System.currentTimeMillis() - begin));

        begin = System.currentTimeMillis();
        Graph result = Transform(graphView);
        logger.debugInfo("transformation-SG", System.currentTimeMillis() - begin);

        begin = System.currentTimeMillis();
        Runtime.getRuntime().gc();
        logger.debugInfo("GC ", (System.currentTimeMillis() - begin));

        begin = System.currentTimeMillis();
        OGDataset sgResult = ConverterUtils.buildSGFromTinkerPopGraph(result);
        logger.debugInfo("ResultTransFromLPG-SG", System.currentTimeMillis() - begin);

        begin = System.currentTimeMillis();
        Runtime.getRuntime().gc();
        logger.debugInfo("GC ", (System.currentTimeMillis() - begin));

        begin = System.currentTimeMillis();
        RMGraph resultRM = ConverterUtils.buildRMFromSG(sgResult, buildRMSchema());
        logger.debugInfo("transTORM-SG", System.currentTimeMillis() - begin);
        logger.info("Lines: " + resultRM.getLines().size());

    }

    @Override
    protected void MG() {
        long begin = System.currentTimeMillis();
        MGraph mGraph = ConverterUtils.buildMGFromTinkerPopGraph(graphData);
        logger.debugInfo("transFromLPG-MG", System.currentTimeMillis() - begin);

        begin = System.currentTimeMillis();
        graphData = null;
        super.forceGC();
        logger.debugInfo("GC ", (System.currentTimeMillis() - begin));

        begin = System.currentTimeMillis();
        Graph graphView = ConverterUtils.buildTinkerPopGraphFromMG(mGraph);
        logger.debugInfo("graph customization-MG", System.currentTimeMillis() - begin);

        begin = System.currentTimeMillis();
        Runtime.getRuntime().gc();
        logger.debugInfo("GC ", (System.currentTimeMillis() - begin));

        begin = System.currentTimeMillis();
        Graph result = Transform(graphView);
        logger.debugInfo("transformation-MG", System.currentTimeMillis() - begin);

        begin = System.currentTimeMillis();
        Runtime.getRuntime().gc();
        logger.debugInfo("GC ", (System.currentTimeMillis() - begin));

        begin = System.currentTimeMillis();
        MGraph mgResult = ConverterUtils.buildMGFromTinkerPopGraph(result);
        logger.debugInfo("ResultTransFromLPG-MG", System.currentTimeMillis() - begin);

        begin = System.currentTimeMillis();
        Runtime.getRuntime().gc();
        logger.debugInfo("GC ", (System.currentTimeMillis() - begin));

        begin = System.currentTimeMillis();
        RMGraph resultRM = ConverterUtils.buildRMFromMG(mgResult, buildRMSchema());
        logger.debugInfo("transTORM-MG", System.currentTimeMillis() - begin);
        logger.info("Lines: " + resultRM.getLines().size());

    }

    @Override
    protected Graph Transform(Graph graph) {
        List<Map<String, Object>> commentsAndPersons = null;
        //查询给2010-05月份发表的帖子回复的用户和其评论
        try {
            commentsAndPersons = graph.traversal().V()
                    .has(T.label, MultiLabelP.of("Forum"))
                    .as("forum")
                    .where(__.out("forum_hasMember_person").count().is(P.gte(20)))
                    .out("forum_containerOf_post")
                    .as("post")
                    .has("creationDate", P.between(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2010-01-01 00:00:00"), new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2010-06-01 00:00:00")))
                    .in("comment_replyOf_post") // 获取评论该Post的Comment
                    .as("comment")
                    .out("comment_hasCreator_person") // 获取Comment的创建者
                    .as("person")
                    .select("person", "comment", "post", "forum").toList();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
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
        return resultGraph;
    }

    @Override
    protected String loggerName() {
        return "FIG14-QUERY-2";
    }

}
