package com.getl.example.query;

import com.getl.constant.CommonConstant;
import com.getl.converter.TinkerPopConverter;
import com.getl.io.LPGParser;
import com.getl.model.LPG.LPGEdge;
import com.getl.model.LPG.LPGGraph;
import com.getl.model.LPG.LPGVertex;
import com.getl.model.ug.UnifiedGraph;
import com.getl.util.DebugUtil;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class Q7 {
    public static void main(String[] args) throws InterruptedException {
        System.out.println("BEGIN TO TEST PG2UGM");
        System.out.println(System.currentTimeMillis());
        String BASE_URL = CommonConstant.LPG_FILES_BASE_URL;
        LPGParser lpgParser = new LPGParser(new TinkerPopConverter(new UnifiedGraph(), null));
        long begin = System.currentTimeMillis();
        String BASE_URL_STATIC = BASE_URL + "static/";
        String BASE_URL_DYNAMIC = BASE_URL + "dynamic/";
        lpgParser.latchSize(8);
        lpgParser.loadVertex(BASE_URL_STATIC + "organisation_0_0.csv", "Organisation").commit2Converter();
        lpgParser.loadVertex(BASE_URL_STATIC + "place_0_0.csv", "Place").commit2Converter();
        lpgParser.loadVertex(BASE_URL_STATIC + "tag_0_0.csv", "Tag").commit2Converter();
        lpgParser.loadVertex(BASE_URL_STATIC + "tagclass_0_0.csv", "TagClass").commit2Converter();
        //DYNAMIC
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "person_0_0.csv", "Person", "birthday", LPGParser.DATE, "creationDate", LPGParser.DATE);
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "person_email_emailaddress_0_0.csv", "Person");
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "person_speaks_language_0_0.csv", "Person").commit2Converter();

        lpgParser.loadVertex(BASE_URL_DYNAMIC + "comment_0_0.csv", "Comment", "creationDate", LPGParser.DATE).commit2Converter();
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "forum_0_0.csv", "Forum", "creationDate", LPGParser.DATE).commit2Converter();
        lpgParser.loadVertex(BASE_URL_DYNAMIC + "post_0_0.csv", "Post", "creationDate", LPGParser.DATE, "length", LPGParser.INT).commit2Converter();
        lpgParser.waitAll();
        //EDGE
        lpgParser.latchSize(23);
        lpgParser.loadEdge(BASE_URL_STATIC + "organisation_isLocatedIn_place_0_0.csv", "organisation_isLocatedIn_place", "Organisation", "Place").commit2Converter();
        lpgParser.loadEdge(BASE_URL_STATIC + "place_isPartOf_place_0_0.csv", "place_isPartOf_place", "Place", "Place").commit2Converter();
        lpgParser.loadEdge(BASE_URL_STATIC + "tag_hasType_tagclass_0_0.csv", "tag_hasType_tagclass", "Tag", "TagClass").commit2Converter();
        lpgParser.loadEdge(BASE_URL_STATIC + "tagclass_isSubclassOf_tagclass_0_0.csv", "tagclass_isSubclassOf_tagclass", "TagClass", "TagClass").commit2Converter();
        //DYNAMIC
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "comment_hasCreator_person_0_0.csv", "comment_hasCreator_person", "Comment", "Person").commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "comment_hasTag_tag_0_0.csv", "comment_hasTag_tag", "Comment", "Tag").commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "comment_isLocatedIn_place_0_0.csv", "comment_isLocatedIn_place", "Comment", "Place").commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "comment_replyOf_comment_0_0.csv", "comment_replyOf_comment", "Comment", "Comment").commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "comment_replyOf_post_0_0.csv", "comment_replyOf_post", "Comment", "Post").commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "forum_containerOf_post_0_0.csv", "forum_containerOf_post", "Forum", "Post").commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "forum_hasMember_person_0_0.csv", "forum_hasMember_person", "Forum", "Person", "joinDate", LPGParser.DATE).commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "forum_hasModerator_person_0_0.csv", "forum_hasModerator_person", "Forum", "Person").commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "forum_hasTag_tag_0_0.csv", "forum_hasTag_tag", "Forum", "Tag").commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_hasInterest_tag_0_0.csv", "person_hasInterest_tag", "Person", "Tag").commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_isLocatedIn_place_0_0.csv", "person_isLocatedIn_place", "Person", "Place").commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_knows_person_0_0.csv", "person_knows_person", "Person", "Person", "creationDate", LPGParser.DATE).commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_likes_comment_0_0.csv", "person_likes_comment", "Person", "Comment", "creationDate", LPGParser.DATE).commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_likes_post_0_0.csv", "person_likes_post", "Person", "Post", "creationDate", LPGParser.DATE).commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_studyAt_organisation_0_0.csv", "person_studyAt_organisation", "Person", "Organisation", "classYear", LPGParser.INT).commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "person_workAt_organisation_0_0.csv", "person_workAt_organisation", "Person", "Organisation", "workFrom", LPGParser.INT).commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "post_hasCreator_person_0_0.csv", "post_hasCreator_person", "Post", "Person").commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "post_hasTag_tag_0_0.csv", "post_hasTag_tag", "Post", "Tag").commit2Converter();
        lpgParser.loadEdge(BASE_URL_DYNAMIC + "post_isLocatedIn_place_0_0.csv", "post_isLocatedIn_place", "Post", "Place").commit2Converter();
//        System.out.println(lpgParser.ids.size());
        DebugUtil.DebugInfo("load pg end " + (System.currentTimeMillis() - begin));
        lpgParser.waitAll();
        DebugUtil.DebugInfo("commit pg end " + (System.currentTimeMillis() - begin));
        Long count = lpgParser.getGraph().traversal().V().hasLabel("Person").count().next();
        System.out.println(count);
        lpgParser.getAsyncPG2UMG().shutdown();
        begin = System.currentTimeMillis();
        RandomWalk randomWalk = new RandomWalk(lpgParser.getGraph());
        LPGGraph resultGraph = new LPGGraph();
        List<List<Vertex>> lists = randomWalk.asyncForward(Math.toIntExact(count), 3);
        System.out.println("rand walk end " + (System.currentTimeMillis() - begin));
        for (int i = 0; i < lists.size(); i++) {
//            System.out.println(i + " : " + lists.get(i).stream().map(Object::toString).collect(Collectors.joining("->")));
            List<Vertex> vertexIds = lists.get(i);
            if (vertexIds.size() < 3) {
                continue;
            }
            Vertex v1 = vertexIds.get(0);
            Vertex e = vertexIds.get(1);
            Vertex v2 = vertexIds.get(2);
            LPGVertex n1V = resultGraph.getOrCreateVertex(v1.id(), v1.label(), v1.properties());
            LPGVertex n2V = resultGraph.getOrCreateVertex(v2.id(), v2.label(), v2.properties());
            LPGEdge lpgEdge = new LPGEdge(resultGraph, n1V, n2V, "recommend");
            lpgEdge.addPropertyValue("post", e.id());
        }
        System.out.println("rand walk end " + (System.currentTimeMillis() - begin));
        double v = 1.0 * (System.currentTimeMillis() - begin) / count;
        System.out.println(v);
    }

}