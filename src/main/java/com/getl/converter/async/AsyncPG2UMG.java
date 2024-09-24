package com.getl.converter.async;

import com.getl.converter.TinkerPopConverter;
import com.getl.model.LPG.LPGVertex;
import com.getl.model.ug.UnifiedGraph;
import com.lmax.disruptor.*;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import lombok.Data;
import lombok.Getter;
import org.apache.tinkerpop.gremlin.structure.*;

import java.util.Iterator;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;


public class AsyncPG2UMG {

    @Data
    public class ElementReference {
        private Element element;
    }

    @Getter
    private UnifiedGraph unifiedGraph;
    private final RingBuffer<ElementReference> ringBuffer;
    private final Disruptor<ElementReference> disruptor;
    @Getter
    private TinkerPopConverter tinkerPopConverter;

    private volatile boolean stopped;

    public void shutdown() {
        addElement(new EventElement());
        System.out.println(stopped);
        while (!stopped) {
            Thread.onSpinWait();
        }
        disruptor.shutdown();
    }

    public AsyncPG2UMG(TinkerPopConverter tinkerPopConverter) {
        this.unifiedGraph = tinkerPopConverter.unifiedGraph;
        this.tinkerPopConverter = tinkerPopConverter;
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        EventFactory<ElementReference> factory = ElementReference::new;
        EventHandler<ElementReference> handler = (element, sequence, endOfBatch) -> {
            if (element.element instanceof EventElement) {
                this.stopped = true;
                return;
            }
            tinkerPopConverter.handleElement(element.element);
        };
        WaitStrategy strategy = new YieldingWaitStrategy();
        int bufferSize = 1024 * 1024;
        disruptor = new Disruptor<>(factory, bufferSize, threadFactory, ProducerType.MULTI, strategy);
        disruptor.handleEventsWith(handler);
        disruptor.start();
        ringBuffer = disruptor.getRingBuffer();
    }

    public void addElement(Element element) {
        long sequence = this.ringBuffer.next();
        try {
            ElementReference event = ringBuffer.get(sequence);
            event.setElement(element);
        } finally {
            ringBuffer.publish(sequence);
        }
    }

    class EventElement implements Element {
        @Override
        public Object id() {
            return null;
        }

        @Override
        public String label() {
            return "";
        }

        @Override
        public Graph graph() {
            return null;
        }

        @Override
        public <V> Property<V> property(String key, V value) {
            return null;
        }

        @Override
        public void remove() {

        }

        @Override
        public <V> Iterator<? extends Property<V>> properties(String... propertyKeys) {
            return null;
        }
    }

}
