package io.techasylum.kafka.statestore.document.internals;

import java.io.File;

import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

public class NoOpReadOnlyStore<K, V> implements ReadOnlyKeyValueStore<K, V>, StateStore {
    private final String name;
    private final boolean rocksdbStore;
    private boolean open = true;
    public boolean initialized;
    public boolean flushed;

    public NoOpReadOnlyStore() {
        this("", false);
    }

    public NoOpReadOnlyStore(final String name) {
        this(name, false);
    }

    public NoOpReadOnlyStore(final String name,
                             final boolean rocksdbStore) {
        this.name = name;
        this.rocksdbStore = rocksdbStore;
    }

    @Override
    public V get(final K key) {
        return null;
    }

    @Override
    public KeyValueIterator<K, V> range(final K from, final K to) {
        return null;
    }

    @Override
    public <PS extends Serializer<P>, P> KeyValueIterator<K, V> prefixScan(P prefix, PS prefixKeySerializer) {
        return null;
    }

    @Override
    public KeyValueIterator<K, V> all() {
        return null;
    }

    @Override
    public long approximateNumEntries() {
        return 0L;
    }

    @Override
    public String name() {
        return name;
    }

    @Deprecated
    @Override
    public void init(final ProcessorContext context, final StateStore root) {
        if (rocksdbStore) {
            // cf. RocksDBStore
            new File(context.stateDir() + File.separator + "rocksdb" + File.separator + name).mkdirs();
        } else {
            new File(context.stateDir() + File.separator + name).mkdir();
        }
        this.initialized = true;
        context.register(root, (k, v) -> { });
    }

    @Override
    public void flush() {
        flushed = true;
    }

    @Override
    public void close() {
        open = false;
    }

    @Override
    public boolean persistent() {
        return rocksdbStore;
    }

    @Override
    public boolean isOpen() {
        return open;
    }

}