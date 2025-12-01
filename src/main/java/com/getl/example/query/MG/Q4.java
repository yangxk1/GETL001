package com.getl.example.query.MG;

import com.getl.model.LPG.LPGGraph;
import com.getl.model.MG.MGraph;
import com.getl.converter.mg.PGMapperR4j;
import org.apache.tinkerpop.gremlin.process.traversal.P;

import static org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__.in;

/**
 * MG 对照实验 Q4: 模拟添加关注关系
 */
public class Q4 extends AbstractMGQuery {
    public static void main(String[] args) {
        new Q4().accept();
    }

    @Override
    protected void run() {
        try {
            logger.debugInfo("BEGIN MG Q4");
            loadSourcePG();
            convertPGToMG();
            preparePGForQuery();
            LPGGraph lpgGraph = wrapAsLPG(pgGraphForQuery);
            long begin = System.currentTimeMillis();
            lpgGraph.traversal().V().hasLabel("Person").as("person1")
                    .in("comment_hasCreator_person").as("comment")
                    .out("comment_replyOf_post").as("post")
                    .out("post_hasCreator_person").as("person2")
                    .where("person1", P.neq("person2"))
                    .not(in("person_isFanOf_person").where(P.eq("person1")))
                    .addE("person_isFanOf_person")
                    .from("person1")
                    .to("person2")
                    .toList();
            logger.debugInfo("query + addE end", (System.currentTimeMillis() - begin));
            begin = System.currentTimeMillis();
            MGraph resultMG = new MGraph();
            new PGMapperR4j(resultMG).addPGToMG(lpgGraph);
            this.mGraph = resultMG;
            logger.debugInfo("Modified LPG -> MG end", (System.currentTimeMillis() - begin));
            exportMGToRDF();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected String getLoggerName() {
        return "MG_QUERY_4";
    }
}
