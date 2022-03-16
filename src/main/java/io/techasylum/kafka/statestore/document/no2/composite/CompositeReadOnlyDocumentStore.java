package io.techasylum.kafka.statestore.document.no2.composite;

import io.techasylum.kafka.statestore.document.ReadOnlyDocumentStore;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.internals.StateStoreProvider;
import org.dizitart.no2.Cursor;
import org.dizitart.no2.Document;
import org.dizitart.no2.Filter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * A wrapper over the underlying {@link ReadOnlyDocumentStore}s found in a {@link
 * org.apache.kafka.streams.processor.internals.ProcessorTopology}
 *
 * @param <K> key type
 */
public class CompositeReadOnlyDocumentStore<K> implements ReadOnlyDocumentStore<K, Document, Cursor, Filter, CompositeFindOptions> {

    private final StateStoreProvider storeProvider;
    private final QueryableStoreType<ReadOnlyDocumentStore<K, Document, Cursor, Filter, CompositeFindOptions>> storeType;
    private final String storeName;

    public CompositeReadOnlyDocumentStore(final StateStoreProvider storeProvider,
                                          final QueryableStoreType<ReadOnlyDocumentStore<K, Document, Cursor, Filter, CompositeFindOptions>> storeType,
                                          final String storeName) {
        this.storeProvider = storeProvider;
        this.storeType = storeType;
        this.storeName = storeName;
    }


    @Override
    public Document get(final K key) {
        Objects.requireNonNull(key);
        final List<ReadOnlyDocumentStore<K, Document, Cursor, Filter, CompositeFindOptions>> stores = storeProvider.stores(storeName, storeType);
        // TODO: use the KeyMetadata to resolve the partition directly and optimize the lookup instead of going through each of the partitions
        for (final ReadOnlyDocumentStore<K, Document, Cursor, Filter, CompositeFindOptions> store : stores) {
            try {
                final Document result = store.get(key);
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
    public Cursor find(Filter filter) {
        Objects.requireNonNull(filter);
        final List<ReadOnlyDocumentStore<K, Document, Cursor, Filter, CompositeFindOptions>> stores = storeProvider.stores(storeName, storeType);
        Map<Integer, Cursor> cursors = new HashMap<>();
        for (final ReadOnlyDocumentStore<K, Document, Cursor, Filter, CompositeFindOptions> store : stores) {
            try {
                final Cursor result = store.find(filter);
                if (result != null) {
                    cursors.put(store.getPartition(), result);
                }
            } catch (final InvalidStateStoreException e) {
                throw new InvalidStateStoreException("State store is not available anymore and may have been migrated to another instance; please re-discover its location from the state metadata.");
            }
        }
        return new CompositeCursor(cursors);
    }

    @Override
    public Cursor findWithOptions(CompositeFindOptions compositeFindOptions) {
        return findWithOptions(null, compositeFindOptions);
    }

    @Override
    public Cursor findWithOptions(Filter filter, CompositeFindOptions compositeFindOptions) {
        Objects.requireNonNull(compositeFindOptions);
        final List<ReadOnlyDocumentStore<K, Document, Cursor, Filter, CompositeFindOptions>> stores = storeProvider.stores(storeName, storeType);
        Map<Integer, Cursor> cursors = new HashMap<>();
        for (final ReadOnlyDocumentStore<K, Document, Cursor, Filter, CompositeFindOptions> store : stores) {
            try {
                int partition = store.getPartition();
                CompositeFindOptions findOptions = compositeFindOptions.getFindOptionsForPartition(partition);
                final Cursor result = store.findWithOptions(filter, findOptions);
                if (result != null) {
                    cursors.put(store.getPartition(), result);
                }
            } catch (final InvalidStateStoreException e) {
                throw new InvalidStateStoreException("State store is not available anymore and may have been migrated to another instance; please re-discover its location from the state metadata.");
            }

        }
        return new CompositeCursor(cursors, compositeFindOptions);
    }

    @Override
    public int getPartition() {
        return 0;
    }
}
