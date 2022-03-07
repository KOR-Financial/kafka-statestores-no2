package io.techasylum.kafka.statestore.document.internals;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.StreamPartitioner;
import org.apache.kafka.streams.processor.internals.RecordCollector;

import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static java.util.Collections.unmodifiableList;

public class MockRecordCollector implements RecordCollector {

    // remember all records that are collected so far
    private final List<ProducerRecord<Object, Object>> collected = new LinkedList<>();

    // remember if flushed is called
    private boolean flushed = false;

    @Override
    public <K, V> void send(final String topic,
                            final K key,
                            final V value,
                            final Headers headers,
                            final Integer partition,
                            final Long timestamp,
                            final Serializer<K> keySerializer,
                            final Serializer<V> valueSerializer) {
        collected.add(new ProducerRecord<>(topic,
            partition,
            timestamp,
            key,
            value,
            headers));
    }

    @Override
    public <K, V> void send(final String topic,
                            final K key,
                            final V value,
                            final Headers headers,
                            final Long timestamp,
                            final Serializer<K> keySerializer,
                            final Serializer<V> valueSerializer,
                            final StreamPartitioner<? super K, ? super V> partitioner) {
        collected.add(new ProducerRecord<>(topic,
            0, // partition id
            timestamp,
            key,
            value,
            headers));
    }

    @Override
    public void initialize() {}

    @Override
    public void flush() {
        flushed = true;
    }

    @Override
    public void closeClean() {}

    @Override
    public void closeDirty() {}

    @Override
    public Map<TopicPartition, Long> offsets() {
        return Collections.emptyMap();
    }

    public List<ProducerRecord<Object, Object>> collected() {
        return unmodifiableList(collected);
    }

    public boolean flushed() {
        return flushed;
    }

    public void clear() {
        this.flushed = false;
        this.collected.clear();
    }
}
