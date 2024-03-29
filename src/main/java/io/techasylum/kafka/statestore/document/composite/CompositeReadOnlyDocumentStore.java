package io.techasylum.kafka.statestore.document.composite;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import io.techasylum.kafka.statestore.document.ReadOnlyCompositeDocumentStore;
import io.techasylum.kafka.statestore.document.ReadOnlyDocumentStore;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.internals.StateStoreProvider;
import org.dizitart.no2.Document;
import org.dizitart.no2.Filter;
import org.dizitart.no2.objects.Cursor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A wrapper over the underlying {@link ReadOnlyCompositeDocumentStore}s found in a {@link
 * org.apache.kafka.streams.processor.internals.ProcessorTopology}
 *
 * @param <Key> key type
 * @param <Doc> teh document type
 */
public class CompositeReadOnlyDocumentStore<Key, Doc extends Document> implements ReadOnlyCompositeDocumentStore<Key, Doc> {

    private static final Logger logger = LoggerFactory.getLogger(CompositeReadOnlyDocumentStore.class);

    private final StateStoreProvider storeProvider;
    private final QueryableStoreType<ReadOnlyDocumentStore<Key, Doc>> storeType;
    private final String storeName;

    public CompositeReadOnlyDocumentStore(final StateStoreProvider storeProvider,
                                          final QueryableStoreType<ReadOnlyDocumentStore<Key, Doc>> storeType,
                                          final String storeName) {
        this.storeProvider = storeProvider;
        this.storeType = storeType;
        this.storeName = storeName;
    }

    @Override
    public Doc get(final Key key) {
        Objects.requireNonNull(key);
        final List<ReadOnlyDocumentStore<Key, Doc>> stores = storeProvider.stores(storeName, storeType);
        // TODO: use the KeyMetadata to resolve the partition directly and optimize the lookup instead of going through each of the partitions
        for (final ReadOnlyDocumentStore<Key, Doc> store : stores) {
            try {
                final Doc result = store.get(key);
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
    public CompositeCursor<Doc> find(Filter filter) {
        Objects.requireNonNull(filter);
        final List<ReadOnlyDocumentStore<Key, Doc>> stores = storeProvider.stores(storeName, null);
        Map<Integer, Cursor<Doc>> cursors = new HashMap<>();
        for (final ReadOnlyDocumentStore<Key, Doc> store : stores) {
            try {
                final Cursor<Doc> result = store.find(filter);
                if (result != null) {
                    cursors.put(store.getPartition(), result);
                }
            } catch (final InvalidStateStoreException e) {
                throw new InvalidStateStoreException("State store is not available anymore and may have been migrated to another instance; please re-discover its location from the state metadata.");
            }
        }
        return CompositeCursor.of(cursors);
    }

    @Override
    public CompositeCursor<Doc> findWithOptions(CompositeFindOptions compositeFindOptions) {
        return findWithOptions(null, compositeFindOptions);
    }

    @Override
    public CompositeCursor<Doc> findWithOptions(Filter filter, CompositeFindOptions compositeFindOptions) {
        Objects.requireNonNull(compositeFindOptions);
        final List<ReadOnlyDocumentStore<Key, Doc>> stores = storeProvider.stores(storeName, null);
        Map<Integer, Cursor<Doc>> cursors = new HashMap<>();
        for (final ReadOnlyDocumentStore<Key, Doc> store : stores) {
            try {
                int partition = store.getPartition();
                CompositeFindOptions findOptions = compositeFindOptions.getFindOptionsForPartition(partition);
                final Cursor<Doc> result = store.findWithOptions(filter, findOptions);
                if (result != null) {
                    cursors.put(store.getPartition(), result);
                }
            } catch (final InvalidStateStoreException e) {
                throw new InvalidStateStoreException("State store is not available anymore and may have been migrated to another instance; please re-discover its location from the state metadata.");
            }

        }
        CompositeCursor<Doc> compositeCursor = CompositeCursor.of(cursors, compositeFindOptions);
        logger.debug("Returning composite cursor: {}", compositeCursor);
        return compositeCursor;
    }
}
