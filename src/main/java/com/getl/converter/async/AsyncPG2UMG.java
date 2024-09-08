package com.getl.converter.async;

import com.getl.converter.TinkerPopConverter;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import lombok.Data;
import org.apache.tinkerpop.gremlin.structure.*;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class AsyncPG2UMG {
    @Data
    public class ElementReference {
        private Element element;
    }

    private final RingBuffer<ElementReference> ringBuffer;
    private final Disruptor<ElementReference> disruptor;

    public void shutdown() {
        disruptor.shutdown();
    }

    public AsyncPG2UMG(TinkerPopConverter tinkerPopConverter) {
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        EventFactory<ElementReference> factory = ElementReference::new;
        EventHandler<ElementReference> handler = (element, sequence, endOfBatch) -> {
            tinkerPopConverter.handleElement(element);
        };
        BlockingWaitStrategy strategy = new BlockingWaitStrategy();
        int bufferSize = 2048;
        disruptor = new Disruptor<>(factory, bufferSize, threadFactory, ProducerType.SINGLE, strategy);
        disruptor.handleEventsWith(handler);
        disruptor.start();
        ringBuffer = disruptor.getRingBuffer();
    }

    public void addVertex(Vertex vertex) {
        long sequence = this.ringBuffer.next();
        try {
            ElementReference event = ringBuffer.get(sequence);
            event.setElement(vertex);
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    public void addEdge(Edge edge) {
        long sequence = this.ringBuffer.next();
        try {
            ElementReference event = ringBuffer.get(sequence);
            event.setElement(edge);
        } finally {
            ringBuffer.publish(sequence);
        }
    }
}
