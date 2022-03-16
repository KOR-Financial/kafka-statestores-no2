package io.techasylum.kafka.statestore.document;

import io.techasylum.kafka.statestore.document.no2.composite.CompositeFindOptions;
import io.techasylum.kafka.statestore.document.no2.composite.CompositeReadOnlyDocumentStore;
import io.techasylum.kafka.statestore.document.object.ReadOnlyObjectDocumentStore;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.internals.StateStoreProvider;
import org.dizitart.no2.Cursor;
import org.dizitart.no2.Document;
import org.dizitart.no2.Filter;

import java.util.Collections;
import java.util.Set;

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
     * A {@link QueryableStoreType} that accepts {@link ReadOnlyObjectDocumentStore}.
     *
     * @param <K> key type of the store
     * @return {@link CompositeDocumentStoreType}
     */
    public static <K> QueryableStoreType<ReadOnlyDocumentStore<K, Document, Cursor, Filter, CompositeFindOptions>> documentStore() {
        return new CompositeDocumentStoreType<>();
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

    public static class CompositeDocumentStoreType<K> extends QueryableDocumentStoreTypes.QueryableStoreTypeMatcher<ReadOnlyDocumentStore<K, Document, Cursor, Filter, CompositeFindOptions>> {

        CompositeDocumentStoreType() {
            super(Collections.singleton(ReadOnlyDocumentStore.class));
        }

        @Override
        public ReadOnlyDocumentStore<K, Document, Cursor, Filter, CompositeFindOptions> create(final StateStoreProvider storeProvider,
                                                                                               final String storeName) {
            return new CompositeReadOnlyDocumentStore<>(storeProvider, this, storeName);
        }

    }

    public static class DocumentObjectStoreType<K, V, F, O> extends QueryableDocumentStoreTypes.QueryableStoreTypeMatcher<ReadOnlyObjectDocumentStore<K, V, F, O>> {

        DocumentObjectStoreType() {
            super(Collections.singleton(ReadOnlyObjectDocumentStore.class));
        }

        @Override
        public ReadOnlyObjectDocumentStore<K, V, F, O> create(final StateStoreProvider storeProvider,
                                                              final String storeName) {
            throw new UnsupportedOperationException("Object stores not supported yet");
        }

    }
}
