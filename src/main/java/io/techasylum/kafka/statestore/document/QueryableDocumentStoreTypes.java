package io.techasylum.kafka.statestore.document;

import java.util.Collections;
import java.util.Set;

import io.techasylum.kafka.statestore.document.composite.CompositeIndexedDocumentStore;
import io.techasylum.kafka.statestore.document.composite.CompositeReadOnlyDocumentStore;
import io.techasylum.kafka.statestore.document.object.ReadOnlyObjectDocumentStore;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.internals.StateStoreProvider;
import org.dizitart.no2.Document;

/**
 * Provides access to the {@link QueryableStoreType}s provided by this library.
 * These can be used with {@link KafkaStreams#store(StoreQueryParameters)}.
 * To access and query the {@link StateStore}s that are part of a {@link Topology}.
 */
public class QueryableDocumentStoreTypes {

    /**
     * A {@link QueryableStoreType} that accepts {@link ReadOnlyObjectDocumentStore}.
     *
     * @param <K> key type of the store
     * @param <V> value type of the store
     * @return {@link CompositeDocumentStoreType}
     */
    public static <K, V, F, O> QueryableStoreType<ReadOnlyObjectDocumentStore<K, V, F, O>> documentObjectStore() {
        throw new UnsupportedOperationException("Object stores not supported yet");
        // return new QueryableDocumentStoreTypes.DocumentObjectStoreType<>();
    }

    /**
     * A {@link QueryableStoreType} that accepts {@link ReadOnlyDocumentStore}.
     *
     * @param <Key> key type of the store
     * @param <Doc> document type of the store
     * @return {@link CompositeDocumentStoreType}
     */
    public static <Key, Doc extends Document> QueryableStoreType<ReadOnlyCompositeDocumentStore<Key, Doc>> documentStore() {
        return new CompositeDocumentStoreType<>();
    }

    /**
     * A {@link QueryableStoreType} that accepts {@link IndexedDocumentStore}.
     *
     * @return {@link CompositeIndexedDocumentStoreType}
     */
    public static QueryableStoreType<IndexedCompositeDocumentStore> indexedDocumentStore() {
        return new CompositeIndexedDocumentStoreType();
    }

    private static abstract class QueryableStoreTypeMatcher<T> implements QueryableStoreType<T> {

        private final Set<Class> matchTo;

        QueryableStoreTypeMatcher(final Set<Class> matchTo) {
            this.matchTo = matchTo;
        }

        @SuppressWarnings("unchecked")
        @Override
        public boolean accepts(final StateStore stateStore) {
            for (final Class matchToClass : matchTo) {
                if (!matchToClass.isAssignableFrom(stateStore.getClass())) {
                    return false;
                }
            }
            return true;
        }
    }

    public static class CompositeDocumentStoreType<Key, Doc extends Document> extends QueryableDocumentStoreTypes.QueryableStoreTypeMatcher<ReadOnlyCompositeDocumentStore<Key, Doc>> {

        CompositeDocumentStoreType() {
            super(Collections.singleton(ReadOnlyDocumentStore.class));
        }

        @Override
        public ReadOnlyCompositeDocumentStore<Key, Doc> create(final StateStoreProvider storeProvider, final String storeName) {
            return new CompositeReadOnlyDocumentStore<>(storeProvider, new DocumentStoreType<>(), storeName);
        }

    }

    static class DocumentStoreType<Key, Doc extends Document> extends QueryableDocumentStoreTypes.QueryableStoreTypeMatcher<ReadOnlyDocumentStore<Key, Doc>> {

        DocumentStoreType() {
            super(Collections.singleton(ReadOnlyDocumentStore.class));
        }

        @Override
        public ReadOnlyDocumentStore<Key, Doc> create(final StateStoreProvider storeProvider, final String storeName) {
            throw new UnsupportedOperationException("Cannot create individual stores through the QueryableStoreTypeMatcher");
        }

    }

    public static class CompositeIndexedDocumentStoreType extends QueryableDocumentStoreTypes.QueryableStoreTypeMatcher<IndexedCompositeDocumentStore> {

        CompositeIndexedDocumentStoreType() {
            super(Collections.singleton(IndexedDocumentStore.class));
        }

        @Override
        public IndexedCompositeDocumentStore create(final StateStoreProvider storeProvider, final String storeName) {
            return new CompositeIndexedDocumentStore(storeProvider, new IndexedDocumentStoreType(), storeName);
        }

    }

    public static class IndexedDocumentStoreType extends QueryableDocumentStoreTypes.QueryableStoreTypeMatcher<IndexedDocumentStore> {

        IndexedDocumentStoreType() {
            super(Collections.singleton(IndexedDocumentStore.class));
        }

        @Override
        public IndexedDocumentStore create(final StateStoreProvider storeProvider, final String storeName) {
            throw new UnsupportedOperationException("Cannot create individual stores through the QueryableStoreTypeMatcher");
        }

    }
}
