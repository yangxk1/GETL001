package com.getl.converter.async;

import com.getl.converter.RMConverter;
import com.getl.converter.TinkerPopConverter;
import com.getl.model.RM.Line;
import com.getl.model.ug.UnifiedGraph;
import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventFactory;
import com.lmax.disruptor.EventHandler;
import com.lmax.disruptor.RingBuffer;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import lombok.Data;
import lombok.Getter;
import org.apache.tinkerpop.gremlin.structure.Element;

import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;

public class AsyncRM2UMG {
    class EventLine extends Line{}
    @Data
    public class LineReference {
        private Line line;
    }

    @Getter
    private UnifiedGraph unifiedGraph;
    private final RingBuffer<LineReference> ringBuffer;
    private final Disruptor<LineReference> disruptor;
    @Getter
    private RMConverter rmConverter;
    private volatile boolean stop;

    public void shutdown() {
        addLine(new EventLine());
        while (!stop) {
            Thread.onSpinWait();
        }
        disruptor.shutdown();
    }

    public AsyncRM2UMG(RMConverter rmConverter) {
        this.unifiedGraph = rmConverter.unifiedGraph;
        this.rmConverter = rmConverter;
        ThreadFactory threadFactory = Executors.defaultThreadFactory();
        EventFactory<LineReference> factory = LineReference::new;
        EventHandler<LineReference> handler = (lineReference, sequence, endOfBatch) -> {
            Line line = lineReference.line;
            if (line instanceof EventLine) {
                stop = true;
                return;
            }
            rmConverter.handleLine(line);
        };
        BlockingWaitStrategy strategy = new BlockingWaitStrategy();
        int bufferSize = 1024 * 1024;
        disruptor = new Disruptor<>(factory, bufferSize, threadFactory, ProducerType.SINGLE, strategy);
        disruptor.handleEventsWith(handler);
        disruptor.start();
        ringBuffer = disruptor.getRingBuffer();
    }

    public void addLine(Line line) {
        long sequence = this.ringBuffer.next();
        try {
            LineReference event = ringBuffer.get(sequence);
            event.setLine(line);
        } finally {
            ringBuffer.publish(sequence);
        }
    }

}
