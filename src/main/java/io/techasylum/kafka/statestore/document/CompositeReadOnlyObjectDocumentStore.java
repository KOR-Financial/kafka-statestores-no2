package io.techasylum.kafka.statestore.document;

import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.internals.StateStoreProvider;

import java.util.List;
import java.util.Objects;

/**
 * A wrapper over the underlying {@link ReadOnlyObjectDocumentStore}s found in a {@link
 * org.apache.kafka.streams.processor.internals.ProcessorTopology}
 *
 * @param <K> key type
 * @param <V> value type
 * @param <F> filter type
 * @param <O> options type
 */
public class CompositeReadOnlyObjectDocumentStore<K, V, F, O> implements ReadOnlyObjectDocumentStore<K, V, F, O> {

    private final StateStoreProvider storeProvider;
    private final QueryableStoreType<ReadOnlyObjectDocumentStore<K, V, F, O>> storeType;
    private final String storeName;

    public CompositeReadOnlyObjectDocumentStore(final StateStoreProvider storeProvider,
                                                final QueryableStoreType<ReadOnlyObjectDocumentStore<K, V, F, O>> storeType,
                                                final String storeName) {
        this.storeProvider = storeProvider;
        this.storeType = storeType;
        this.storeName = storeName;
    }


    @Override
    public V get(final K key) {
        Objects.requireNonNull(key);
        final List<ReadOnlyObjectDocumentStore<K, V, F, O>> stores = storeProvider.stores(storeName, storeType);
        // TODO: use the KeyMetadata to resolve the partition directly and optimize the lookup instead of going through each of the partitions
        for (final ReadOnlyObjectDocumentStore<K, V, F, O> store : stores) {
            try {
                final V result = store.get(key);
                if (result != null) {
                    return result;
                }
            } catch (final InvalidStateStoreException e) {
                throw new InvalidStateStoreException("State store is not available anymore and may have been migrated to another instance; please re-discover its location from the state metadata.");
            }

        }
        return null;
    }

    @Override
    public QueryCursor<V> find(F filter) {
        Objects.requireNonNull(filter);
        final List<ReadOnlyObjectDocumentStore<K, V, F, O>> stores = storeProvider.stores(storeName, storeType);
        for (final ReadOnlyObjectDocumentStore<K, V, F, O> store : stores) {
            try {
                final QueryCursor<V> result = store.find(filter);
                // TODO: merge queryCursors of all partitions
                if (result != null) {
                    return result;
                }
            } catch (final InvalidStateStoreException e) {
                throw new InvalidStateStoreException("State store is not available anymore and may have been migrated to another instance; please re-discover its location from the state metadata.");
            }

        }
        return null;
    }

    @Override
    public QueryCursor<V> findWithOptions(O options) {
        Objects.requireNonNull(options);
        final List<ReadOnlyObjectDocumentStore<K, V, F, O>> stores = storeProvider.stores(storeName, storeType);
        for (final ReadOnlyObjectDocumentStore<K, V, F, O> store : stores) {
            try {
                final QueryCursor<V> result = store.findWithOptions(options);
                // TODO: merge queryCursors of all partitions
                if (result != null) {
                    return result;
                }
            } catch (final InvalidStateStoreException e) {
                throw new InvalidStateStoreException("State store is not available anymore and may have been migrated to another instance; please re-discover its location from the state metadata.");
            }

        }
        return null;
    }

    @Override
    public QueryCursor<V> findWithOptions(F filter, O options) {
        Objects.requireNonNull(filter);
        Objects.requireNonNull(options);
        final List<ReadOnlyObjectDocumentStore<K, V, F, O>> stores = storeProvider.stores(storeName, storeType);
        for (final ReadOnlyObjectDocumentStore<K, V, F, O> store : stores) {
            try {
                final QueryCursor<V> result = store.findWithOptions(filter, options);
                // TODO: merge queryCursors of all partitions
                if (result != null) {
                    return result;
                }
            } catch (final InvalidStateStoreException e) {
                throw new InvalidStateStoreException("State store is not available anymore and may have been migrated to another instance; please re-discover its location from the state metadata.");
            }

        }
        return null;
    }

}
