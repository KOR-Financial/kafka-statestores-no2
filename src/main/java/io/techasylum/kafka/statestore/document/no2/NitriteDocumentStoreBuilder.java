package io.techasylum.kafka.statestore.document.no2;

import java.util.HashMap;
import java.util.Map;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.state.StoreBuilder;
import org.dizitart.no2.Document;
import org.dizitart.no2.IndexOptions;
import org.dizitart.no2.IndexType;

public class NitriteDocumentStoreBuilder<K> implements StoreBuilder<NitriteDocumentStore<K>> {

    private final String name;
    private final Serde<K> keySerde;
    private final Serde<Document> valueSerde;
    private final String keyFieldName;

    private Map<String, String> logConfig = new HashMap<>();
    private Map<String, IndexOptions> indices = new HashMap<>();

    public NitriteDocumentStoreBuilder(String name, Serde<K> keySerde, Serde<Document> valueSerde, String keyFieldName) {
        this.name = name;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.keyFieldName = keyFieldName;
    }

    @Override
    public NitriteDocumentStoreBuilder<K> withCachingEnabled() {
        throw new UnsupportedOperationException("caching is not available for nitrite stores");
    }

    @Override
    public NitriteDocumentStoreBuilder<K> withCachingDisabled() {
        return this;
    }

    @Override
    public NitriteDocumentStoreBuilder<K> withLoggingEnabled(Map<String, String> config) {
        this.logConfig = config;
        return this;
    }

    @Override
    public NitriteDocumentStoreBuilder<K> withLoggingDisabled() {
        throw new UnsupportedOperationException("logging cannot be turned off for nitrite stores");
    }

    /**
     * Asynchronously adds an index to the document store.
     *
     * Use the {@link #withUniqueIndexOn(String) withUniqueIndexOn} method to add a unique index to the document store.
     * Use the {@link #withFullTextIndexOn(String) withFullTextIndexOn} method for full text search.
     *
     * If you want more flexibility you should use the {@link #withIndex(String, IndexOptions) withIndex} method.
     *
     * @param field the field to index
     * @return the Nitrite document store builder
     * @see #withUniqueIndexOn(String)
     * @see #withFullTextIndexOn(String)
     * @see #withIndex(String, IndexOptions)
     */
    public NitriteDocumentStoreBuilder<K> withIndexOn(String field) {
        return withIndex(field, IndexOptions.indexOptions(IndexType.NonUnique, true));
    }

    /**
     * Asynchronously adds a unique index to the document store.
     *
     * Use the {@link #withIndexOn(String) withIndexOn} method to add an index to the document store.
     * Use the {@link #withFullTextIndexOn(String) withFullTextIndexOn} method for full text search.
     *
     * If you want more flexibility you should use the {@link #withIndex(String, IndexOptions) withIndex} method.
     *
     * @param field the field to index
     * @return the Nitrite document store builder
     * @see #withIndexOn(String)
     * @see #withFullTextIndexOn(String)
     * @see #withIndex(String, IndexOptions)
     */
    public NitriteDocumentStoreBuilder<K> withUniqueIndexOn(String field) {
        return withIndex(field, IndexOptions.indexOptions(IndexType.Unique, true));
    }

    /**
     * Asynchronously adds a full-text index to the document store.
     *
     * Use the {@link #withIndexOn(String) withIndexOn} method to add an index to the document store.
     * Use the {@link #withUniqueIndexOn(String) withUniqueIndex} method to add a unique index to the document store.
     *
     * If you want more flexibility you should use the {@link #withIndex(String, IndexOptions) withIndex} method.
     *
     * @param field the field to index
     * @return the Nitrite document store builder
     * @see #withIndexOn(String)
     * @see #withUniqueIndexOn(String)
     * @see #withIndex(String, IndexOptions)
     */
    public NitriteDocumentStoreBuilder<K> withFullTextIndexOn(String field) {
        return withIndex(field, IndexOptions.indexOptions(IndexType.Fulltext, true));
    }

    /**
     * Adds an index to the document store with the given options.
     *
     * @param field the field to index
     * @param options the options to create an index
     * @return the Nitrite document store builder
     * @see #withIndexOn(String)
     * @see #withUniqueIndexOn(String)
     * @see #withFullTextIndexOn(String)
     * @see #withIndex(String, IndexOptions)
     */
    public NitriteDocumentStoreBuilder<K> withIndex(String field, IndexOptions options) {
        indices.put(field, options);
        return this;
    }

    @Override
    public NitriteDocumentStore<K> build() {
        return new NitriteDocumentStore(this.name, this.keySerde, this.valueSerde, this.keyFieldName, indices);
    }

    @Override
    public Map<String, String> logConfig() {
        return logConfig;
    }

    @Override
    public boolean loggingEnabled() {
        return true;
    }

    @Override
    public String name() {
        return this.name;
    }
}
