package io.techasylum.kafka.statestore.document.object;

import java.util.List;
import java.util.Map;

public interface QueryCursor<V> extends Iterable<V> {

    /**
     * Specifies if there are more elements in the store that
     * has not been retrieved yet.
     *
     * @return `true` if the cursor has more elements; otherwise `false`.
     */
    boolean hasMore();

    /**
     * Gets the size of the current record set.
     *
     * @return the size of the current record set.
     */
    int size();

    /**
     * Gets the total count of the records in the store matching a filter criteria.
     *
     * @return total count of matching documents.
     */
    int totalCount();

    /**
     * Gets the first element of the result or
     * `null` if it is empty.
     *
     * @return the first element or `null`
     */
    V firstOrDefault();

    /**
     * Returns a list of all elements.
     *
     * @return list of all elements.
     * */
    List<V> toList();

    Map<Integer, Integer> getNextOffsets();
}
