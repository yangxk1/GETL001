package com.getl.example.async;

import com.getl.constant.CommonConstant;
import com.getl.constant.IRINamespace;
import com.getl.converter.LPGGraphConverter;
import com.getl.converter.TinkerPopConverter;
import com.getl.example.Runnable;
import com.getl.io.LPGParser;
import com.getl.model.LPG.LPGGraph;
import com.getl.model.ug.BasePair;
import com.getl.model.ug.IRI;
import com.getl.model.ug.NestedPair;
import com.getl.model.ug.UnifiedGraph;
import com.getl.util.DebugUtil;
import org.apache.commons.lang3.StringUtils;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.tinkerpop.gremlin.process.traversal.Order.decr;

public class AsyncPG2UGTest extends Runnable {

    public static AtomicInteger inCount = new AtomicInteger(0);
    public static AtomicInteger outCount = new AtomicInteger(0);
    public static AtomicInteger count = new AtomicInteger(0);

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
        System.out.println("pg count:" + lpgParser.getGraph().traversal().V().count().next());
        System.out.println(lpgParser.getGraph().traversal().E().count().next());
        Set<Object> set1 = lpgParser.getGraph().traversal().E().id().toSet().stream().map(i -> i.toString()).collect(Collectors.toSet());
        System.out.println(set1.size());
        lpgParser.getAsyncPG2UMG().shutdown();
        DebugUtil.DebugInfo("convert to ugm end " + (System.currentTimeMillis() - begin));
        UnifiedGraph unifiedGraph = lpgParser.getAsyncPG2UMG().getUnifiedGraph();
        System.out.println("ugm edge count:" + unifiedGraph.getCache().stream().map(NestedPair::from).filter(basePair -> basePair.getLabels().stream().findFirst().map(IRI::getNameSpace).orElse("").equals(IRINamespace.EDGE_NAMESPACE)).map(BasePair::getValueIRI).map(IRI::getLocalName).collect(Collectors.toSet()).size());
        lpgParser.setGraph(null);
        lpgParser.setAsyncPG2UMG(null);
        lpgParser = null;
        Runtime.getRuntime().gc();
        System.out.println("pg count:" + unifiedGraph.traversal().V().count().next());
        System.out.println(unifiedGraph.getCache().size());
        // System.out.println(unifiedGraph.traversal().V().outE().outV().outE().dedup().limit(100).toList());
        LPGGraph lpgGraphByUGM = new LPGGraphConverter(unifiedGraph, null, new HashMap<>()).createLPGGraphByUGM();
        System.out.println(lpgGraphByUGM.getVertices().size());
        System.out.println(lpgGraphByUGM.getEdges().size());
        Set<String> set2 = lpgGraphByUGM.traversal().E().id().toSet().stream().map(i -> i.toString()).collect(Collectors.toSet());
        set1.removeAll(set2);
        System.out.println(set1);
    }

    @Override
    public String init() {
        if (StringUtils.isBlank(CommonConstant.LPG_FILES_BASE_URL)) {
            return CommonConstant.LPG_FILES_BASE_URL;
        }
        return "";
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
