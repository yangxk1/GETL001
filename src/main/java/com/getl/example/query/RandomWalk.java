package com.getl.example.query;

import org.apache.commons.collections.list.SynchronizedList;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.GraphTraversalSource;
import org.apache.tinkerpop.gremlin.structure.*;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class RandomWalk {

    private long countTime = 0;
    private GraphTraversalSource g;
    private Graph graph;
    List<List<Object>> order;

    public RandomWalk(Graph graph) {
        this.graph = graph;
        this.g = this.graph.traversal();
    }

    public List<List<Object>> forward(int order_size, int maxSteps) {
        List<Vertex> vertexList = graph.traversal().V().hasLabel("Person").toList();
        order = new ArrayList<>(order_size);
        for (int i = 0; i < order_size; i++) {
            vertexList.stream().skip(new Random().nextInt(vertexList.size())).findAny().ifPresent(person -> randomWalk(person.id(), maxSteps));
        }
        return order;
    }

    public List<List<Object>> asyncForward(int order_size, int maxSteps) {
        countTime = 0;
        List<Vertex> vertexList = graph.traversal().V().hasLabel("Person").toList();
        order = Collections.synchronizedList(new ArrayList<>(order_size));
        int poolSize = 8;
//        ExecutorService executorService = Executors.newFixedThreadPool(poolSize);
        CountDownLatch countDownLatch = new CountDownLatch(poolSize);
        try {
            for (int i = 1; i <= poolSize; i++) {
                final int index = i;
                System.out.println(Thread.currentThread().getName() + " " + (index - 1) * order_size / poolSize + "_____" + index * order_size / poolSize);
                new Thread(() -> {
                    for (int j = (index - 1) / poolSize * order_size; j < index / poolSize * order_size; j++) {
                        randomWalk(vertexList.get(j).id(), maxSteps);
                    }
                    countDownLatch.countDown();
                }).start();
            }
            countDownLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        System.out.println("order size: " + order.size());
        return order;
    }

    private void randomWalk(Object startVertexId, int maxSteps) {
        Object currentPeopleId = startVertexId;
        Object currentPostId = null;
        List<Object> visited = new ArrayList<>();
        visited.add(currentPeopleId);
        for (int step = 0; step < maxSteps / 3; step++) {
            //person -> post
            Vertex currentVertex_person = g.V(currentPeopleId).next();
            List<Map.Entry<Vertex, Double>> person_post_neighbors = new ArrayList<>();
            g.V(currentPeopleId).out("person_likes_post", "post_hasCreator_person").forEachRemaining(neighbor -> {
                double weightedValue = calculateNePostWeight(currentVertex_person, neighbor);
                person_post_neighbors.add(new AbstractMap.SimpleEntry<>(neighbor, weightedValue));
            });

            // If no neighbors, break the loop
            if (person_post_neighbors.isEmpty()) {
                break;
            }

            // Randomly select the next vertex based on weights
            currentPostId = selectWeightedRandomVertex(person_post_neighbors);
            //post -> person
            visited.add(currentPostId);
            Vertex currentVertex_post = g.V(currentPostId).next();
            List<Map.Entry<Vertex, Double>> post_person_neighbors = new ArrayList<>();
            g.V(currentPostId).in("person_likes_post", "post_hasCreator_person").forEachRemaining(neighbor -> {
                double weightedValue = calculateNePersonWeight(currentVertex_post, neighbor);
                if (!neighbor.equals(currentVertex_person)) {
                    post_person_neighbors.add(new AbstractMap.SimpleEntry<>(neighbor, weightedValue));
                }
            });
            // If no neighbors, break the loop
            if (post_person_neighbors.isEmpty()) {
                break;
            }
            currentPeopleId = selectWeightedRandomVertex(post_person_neighbors);
            visited.add(currentPeopleId);
        }
        order.add(visited);
    }

    /**
     * @param currentVertexPost post vertex
     * @param neighbor          person vertex
     */
    private double calculateNePersonWeight(Vertex currentVertexPost, Vertex neighbor) {
        double weight = 0.0;
        boolean personKnowsPerson = g.V(currentVertexPost.id()).both("person_knows_person").has(T.id, neighbor.id()).hasNext();
        if (personKnowsPerson) {
            weight += 10.0;
        }
        weight += calculateNePostWeight(neighbor, currentVertexPost);
        return weight;
    }

    /**
     * @param vertex   person vertex
     * @param neighbor post vertex
     */
    private double calculateNePostWeight(Vertex vertex, Vertex neighbor) {
        double weight = 0.0;
        double recentScore = getRecentScore(neighbor);
        weight += recentScore * (getInteractionCount(neighbor) + getTagsSimilarity(vertex, neighbor));

        return weight;
    }

    private double getTagsSimilarity(Vertex vertex, Vertex neighbor) {
        HashSet<Object> userTags = new HashSet<>(g.V(vertex.id()).out("person_hasInterest_tag").id().toList());
        HashSet<Object> commentTags = new HashSet<>(g.V(vertex.id()).out("post_hasTag_tag").id().toList());

        // 计算相似度
        int commonTagsCount = 0;
        for (Object tag : commentTags) {
            if (userTags.contains(tag)) {
                commonTagsCount++;
            }
        }
        return commonTagsCount;
    }

    //点赞评论
    private double getInteractionCount(Vertex neighbor) {
        Long likeCount = g.V(neighbor.id()).in("person_likes_post").count().next();
        Long replyCount = g.V(neighbor.id()).in("comment_replyOf_post").count().next();
        return (likeCount * 1.0) + (replyCount * 2.0);
    }

    //时间相关性
    private double getRecentScore(Vertex neighbor) {
        LocalDateTime now = LocalDateTime.now();
        VertexProperty<Object> createTime = neighbor.property("create_time");
        if (!createTime.isPresent()) {
            return 0.1;
        }
        Date date = (Date) createTime.value();
        LocalDateTime creationDate = Instant.ofEpochMilli(date.getTime())
                .atZone(ZoneId.systemDefault())
                .toLocalDateTime();
        long daysBetween = ChronoUnit.DAYS.between(creationDate, now);
        // 假设最近的评论得分最高，分数随着时间增长而减少
        // 例如：一天内得10分，超过7天则得分线性下降
        if (daysBetween < 1) {
            return 10.0;
        } else if (daysBetween < 7) {
            return 10.0 - (daysBetween * (10.0 / 10)); // 线性下降
        } else {
            return 0.1; // 超过一周的评论推荐型最低
        }
    }

    private Object selectWeightedRandomVertex(List<Map.Entry<Vertex, Double>> weightedNeighbors) {
        double totalWeight = weightedNeighbors.stream().mapToDouble(Map.Entry::getValue).sum();
        double randomValue = Math.random() * totalWeight;
        double cumulativeWeight = 0.0;

        for (Map.Entry<Vertex, Double> entry : weightedNeighbors) {
            cumulativeWeight += entry.getValue();
            if (cumulativeWeight >= randomValue) {
                return entry.getKey().id(); // Return the vertex ID
            }
        }
        return -1; // Should not reach here
    }
}
