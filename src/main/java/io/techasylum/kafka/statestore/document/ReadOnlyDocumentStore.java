package io.techasylum.kafka.statestore.document;

import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.dizitart.no2.Document;
import org.dizitart.no2.Filter;
import org.dizitart.no2.FindOptions;
import org.dizitart.no2.objects.Cursor;

public interface ReadOnlyDocumentStore<Key, Doc extends Document> {

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
    Cursor<Doc> find(Filter filter);

    /**
     * Returns a customized cursor to all objects in the store.
     *
     * @param options specifies pagination, sort options for the cursor.
     * @return a cursor to all selected objects.
     * @throws NullPointerException if `findOptions` is null.
     */
    Cursor<Doc> findWithOptions(FindOptions options);

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
     * @throws NullPointerException if `findOptions` is null.
     */
    Cursor<Doc> findWithOptions(Filter filter, FindOptions options);


    /**
     * Get the value corresponding to this key.
     *
     * @param key The key to fetch
     * @return The value or null if no value is found.
     * @throws NullPointerException if `key` is null.
     * @throws InvalidStateStoreException if the store is not initialized
     */
    Doc get(Key key);

    int getPartition();

}
