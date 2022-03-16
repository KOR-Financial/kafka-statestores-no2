package io.techasylum.kafka.statestore.document.object;

import org.apache.kafka.streams.errors.InvalidStateStoreException;

public interface ReadOnlyObjectDocumentStore<K, V, F, O> {

    /**
     * Applies a filter on the store and returns a cursor to the
     * selected objects.
     *
     * NOTE: If there is an index on the value specified in the filter, this operation
     * will take advantage of the index.
     *
     * @param filter the filter to apply to select objects from the store.
     * @return a cursor to all selected objects.
     * @throws NullPointerException if `filter` is null.
     */
    QueryCursor<V> find(F filter);

    /**
     * Returns a customized cursor to all objects in the store.
     *
     * @param options specifies pagination, sort options for the cursor.
     * @return a cursor to all selected objects.
     * @throws NullPointerException if `findOptions` is null.
     */
    QueryCursor<V> findWithOptions(O options);

    /**
     * Applies a filter on the store and returns a customized cursor to the
     * selected objects.
     *
     * NOTE: If there is an index on the value specified in the filter, this operation
     * will take advantage of the index.
     *
     * @param filter      the filter to apply to select objects from collection.
     * @param options specifies pagination, sort options for the cursor.
     * @return a cursor to all selected objects.
     * @throws NullPointerException if `filter` is null.
     * @throws NullPointerException if `findOptions` is null.
     */
    QueryCursor<V> findWithOptions(F filter, O options);


    /**
     * Get the value corresponding to this key.
     *
     * @param key The key to fetch
     * @return The value or null if no value is found.
     * @throws NullPointerException if `key` is null.
     * @throws InvalidStateStoreException if the store is not initialized
     */
    V get(K key);

}
