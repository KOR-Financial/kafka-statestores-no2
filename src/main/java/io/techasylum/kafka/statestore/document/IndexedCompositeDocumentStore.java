package io.techasylum.kafka.statestore.document;

import org.dizitart.no2.Index;
import org.dizitart.no2.IndexOptions;
import org.dizitart.no2.IndexType;
import org.dizitart.no2.exceptions.IndexingException;

import java.util.Collection;
import java.util.Map;

public interface IndexedCompositeDocumentStore {

    /**
     * Creates an index on `value`, if not already exists.
     * If `indexOptions` is `null`, it will use default options.
     * <p>
     * The default indexing option is -
     * <p>
     * - `indexOptions.setAsync(false);`
     * - `indexOptions.setIndexType(IndexType.Unique);`
     * <p>
     * [icon="{@docRoot}/note.png"]
     * [NOTE]
     * ====
     * - '_id' value of the document is always indexed. But full text
     * indexing is not supported on '_id' value.
     * - Compound index is not supported.
     * - Indexing on arrays or collection is not supported
     * - Indexing on non-comparable value is not supported
     * ====
     *
     * @param field        the value to be indexed.
     * @param indexOptions index options.
     * @throws IndexingException if an index already exists on `value`.
     * @see IndexOptions
     * @see IndexType
     */
    void createIndex(String field, IndexOptions indexOptions);

    /**
     * Rebuilds index on `field` if it exists.
     *
     * @param field the value to be indexed.
     * @param async if set to `true`, the indexing will run in background; otherwise, in foreground.
     * @throws IndexingException if the `field` is not indexed.
     */
    void rebuildIndex(String field, boolean async);

    /**
     * Gets a set of all indices in the collection.
     *
     * @return a set of all indices.
     * @see Index
     */
    Map<Integer, Collection<Index>> listIndices();

    /**
     * Checks if a value is already indexed or not.
     *
     * @param field the value to check.
     * @return `true` if the `value` is indexed; otherwise, `false`.
     */
    Map<Integer, Boolean> hasIndex(String field);

    /**
     * Checks if indexing operation is currently ongoing for a `field`.
     *
     * @param field the value to check.
     * @return `true` if indexing is currently running; otherwise, `false`.
     */
    Map<Integer, Boolean> isIndexing(String field);

    /**
     * Drops the index on a `field`.
     *
     * @param field the index of the `field` to drop.
     * @throws IndexingException if indexing is currently running on the `field`.
     * @throws IndexingException if the `field` is not indexed.
     */
    void dropIndex(String field);

    /**
     * Drops all indices from the collection.
     *
     * @throws IndexingException if indexing is running on any value.
     */
    void dropAllIndices();
}
