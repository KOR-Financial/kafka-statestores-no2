package io.techasylum.kafka.statestore.document;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.processor.StateStore;
import org.dizitart.no2.Document;

import java.util.List;

public interface WritableDocumentStore<Key> extends StateStore, ReadOnlyDocumentStore<Key> {

    /**
     * Update the value associated with this key.
     *
     * @param key   The key to associate the value to
     * @param value The value to update, it can be {@code null};
     *              if the serialized bytes are also {@code null} it is interpreted as deletes
     * @throws NullPointerException If {@code null} is used for key.
     */
    void put(Key key, Document value);

    /**
     * Update the value associated with this key, unless a value is already associated with the key.
     *
     * @param key   The key to associate the value to
     * @param value The value to update, it can be {@code null};
     *              if the serialized bytes are also {@code null} it is interpreted as deletes
     * @return The old value or {@code null} if there is no such key.
     * @throws NullPointerException If {@code null} is used for key.
     */
    Document putIfAbsent(Key key, Document value);

    /**
     * Update all the given key/value pairs.
     *
     * @param entries A list of entries to put into the store;
     *                if the serialized bytes are also {@code null} it is interpreted as deletes
     * @throws NullPointerException If {@code null} is used for key.
     */
    void putAll(List<KeyValue<Key, Document>> entries);

    /**
     * Delete the value from the store (if there is one).
     *
     * @param key The key
     * @return The old value or {@code null} if there is no such key.
     * @throws NullPointerException If {@code null} is used for key.
     */
    Document delete(Key key);
}
