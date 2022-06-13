package io.techasylum.kafka.statestore.document.composite;

import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import io.techasylum.kafka.statestore.document.IndexedCompositeDocumentStore;
import io.techasylum.kafka.statestore.document.IndexedDocumentStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.internals.StateStoreProvider;
import org.dizitart.no2.Index;
import org.dizitart.no2.IndexOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.stream.Collectors.toMap;

/**
 * A wrapper over the underlying {@link IndexedCompositeDocumentStore}s found in a {@link
 * org.apache.kafka.streams.processor.internals.ProcessorTopology}
 */
public class CompositeIndexedDocumentStore implements IndexedCompositeDocumentStore {

    private static final Logger logger = LoggerFactory.getLogger(CompositeIndexedDocumentStore.class);

    private final StateStoreProvider storeProvider;
    private final QueryableStoreType<IndexedDocumentStore> storeType;
    private final String storeName;

    public CompositeIndexedDocumentStore(final StateStoreProvider storeProvider,
                                         final QueryableStoreType<IndexedDocumentStore> storeType,
                                         final String storeName) {
        this.storeProvider = storeProvider;
        this.storeType = storeType;
        this.storeName = storeName;
    }

    @Override
    public void createIndex(String field, IndexOptions indexOptions) {
        storeProvider.stores(storeName, storeType).forEach((store) -> store.createIndex(field, indexOptions));
    }

    @Override
    public void rebuildIndex(String field, boolean async) {
        storeProvider.stores(storeName, storeType).forEach((store) -> store.rebuildIndex(field, async));
    }

    @Override
    public Map<Integer, Collection<Index>> listIndices() {
        return storeProvider.stores(storeName, storeType).stream()
                .map(store -> Map.entry(store.getPartition(), store.listIndices()))
                .collect(toMap(Entry::getKey, Entry::getValue));
    }

    @Override
    public Map<Integer, Boolean> hasIndex(String field) {
        return storeProvider.stores(storeName, storeType).stream()
                .map(store -> Map.entry(store.getPartition(), store.hasIndex(field)))
                .collect(toMap(Entry::getKey, Entry::getValue));
    }

    @Override
    public Map<Integer, Boolean> isIndexing(String field) {
        return storeProvider.stores(storeName, storeType).stream()
                .map(store -> Map.entry(store.getPartition(), store.isIndexing(field)))
                .collect(toMap(Entry::getKey, Entry::getValue));
    }

    @Override
    public void dropIndex(String field) {
        storeProvider.stores(storeName, storeType).forEach((store) -> store.dropIndex(field));
    }

    @Override
    public void dropAllIndices() {
        storeProvider.stores(storeName, storeType).forEach(IndexedDocumentStore::dropAllIndices);
    }
}
