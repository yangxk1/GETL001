package com.getl.example;

import com.getl.constant.CommonConstant;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class FlashData {
    public static void main(String[] args) {
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
}
