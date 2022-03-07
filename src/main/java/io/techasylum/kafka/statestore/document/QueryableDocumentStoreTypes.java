package io.techasylum.kafka.statestore.document;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.internals.StateStoreProvider;

import java.util.Collections;
import java.util.Set;

/**
 * Provides access to the {@link QueryableStoreType}s provided by this library.
 * These can be used with {@link KafkaStreams#store(StoreQueryParameters)}.
 * To access and query the {@link StateStore}s that are part of a {@link Topology}.
 */
public class QueryableDocumentStoreTypes {

    /**
     * A {@link QueryableStoreType} that accepts {@link ReadOnlyDocumentStore}.
     *
     * @param <K> key type of the store
     * @param <V> value type of the store
     * @return {@link QueryableDocumentStoreTypes.DocumentStoreType}
     */
    public static <K, V, F, O> QueryableStoreType<ReadOnlyDocumentStore<K, V, F, O>> documentStore() {
        return new QueryableDocumentStoreTypes.DocumentStoreType<>();
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

    public static class DocumentStoreType<K, V, F, O> extends QueryableDocumentStoreTypes.QueryableStoreTypeMatcher<ReadOnlyDocumentStore<K, V, F, O>> {

        DocumentStoreType() {
            super(Collections.singleton(ReadOnlyDocumentStore.class));
        }

        @Override
        public ReadOnlyDocumentStore<K, V, F, O> create(final StateStoreProvider storeProvider,
                                                  final String storeName) {
            return new CompositeReadOnlyDocumentStore<>(storeProvider, this, storeName);
        }

    }
}
