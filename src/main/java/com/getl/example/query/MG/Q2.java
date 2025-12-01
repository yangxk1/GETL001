package com.getl.example.query.MG;

import com.getl.query.step.MultiLabelP;
import com.getl.model.LPG.LPGEdge;
import com.getl.model.LPG.LPGGraph;
import com.getl.model.LPG.LPGVertex;
import com.getl.model.MG.MGraph;
import com.getl.converter.mg.PGMapperR4j;
import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.text.SimpleDateFormat;
import java.util.*;

import static org.apache.tinkerpop.gremlin.process.traversal.P.between;
import static org.apache.tinkerpop.gremlin.structure.T.label;

/**
 * MG 对照实验 Q2: Model -> MG -> Query(on LPG wrapped from MG) -> Result(MG) -> RDF
 */
public class Q2 extends AbstractMGQuery {
    public static void main(String[] args) {
        new Q2().accept();
    }

    @Override
    @SuppressWarnings("unchecked")
    protected void run() {
        try {
            logger.debugInfo("BEGIN MG Q2");
            loadSourcePG();
            convertPGToMG();
            preparePGForQuery();
            LPGGraph lpgGraph = wrapAsLPG(pgGraphForQuery);
            long begin = System.currentTimeMillis();
            List<Map<String, Object>> commentsAndPersons = lpgGraph.traversal().V()
                    .has(label, MultiLabelP.of("Forum"))
                    .as("forum")
                    .where(__.out("forum_hasMember_person").count().is(P.gte(20)))
                    .out("forum_containerOf_post")
                    .as("post")
                    .has("creationDate", between(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2010-01-01 00:00:00"), new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").parse("2010-06-01 00:00:00")))
                    .in("comment_replyOf_post")
                    .as("comment")
                    .out("comment_hasCreator_person")
                    .as("person")
                    .select("person", "comment", "post", "forum").toList();
            logger.debugInfo("query end", (System.currentTimeMillis() - begin));
            begin = System.currentTimeMillis();
            LPGGraph resultGraph = new LPGGraph();
            for (Object result : commentsAndPersons) {
                Map<String, Object> vertexMap = (Map<String, Object>) result;
                Vertex person = (Vertex) vertexMap.get("person");
                Vertex comment = (Vertex) vertexMap.get("comment");
                Vertex forum = (Vertex) vertexMap.get("forum");
                Vertex post = (Vertex) vertexMap.get("post");
                LPGVertex forumV = resultGraph.getOrCreateVertex(forum.id(), forum.label(), forum.properties());
                LPGVertex postV = resultGraph.getOrCreateVertex(post.id(), post.label(), post.properties());
                new LPGEdge(resultGraph, forumV, postV, "forum_containerOf_post");
                LPGVertex personV = resultGraph.getOrCreateVertex(person.id(), person.label(), person.properties());
                LPGVertex commentV = resultGraph.getOrCreateVertex(comment.id(), comment.label(), comment.properties());
                new LPGEdge(resultGraph, commentV, postV, "comment_replyOf_post");
                new LPGEdge(resultGraph, personV, commentV, "comment_hasCreator_person");
            }
            logger.debugInfo("collect result to lpg end", (System.currentTimeMillis() - begin));
            begin = System.currentTimeMillis();
            // 将结果图转换回 MG
            MGraph resultMG = new MGraph();
            new PGMapperR4j(resultMG).addPGToMG(resultGraph);
            this.mGraph = resultMG;
            logger.debugInfo("Result LPG -> MG end", (System.currentTimeMillis() - begin));
            // 导出 RDF 以得到最终模型
            exportMGToRDF();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected String getLoggerName() {
        return "MG_QUERY_2";
    }
}
