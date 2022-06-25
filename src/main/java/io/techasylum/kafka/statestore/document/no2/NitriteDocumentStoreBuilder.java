package io.techasylum.kafka.statestore.document.no2;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.techasylum.kafka.statestore.document.serialization.DocumentSerde;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.state.StoreBuilder;
import org.dizitart.no2.Document;
import org.dizitart.no2.IndexOptions;
import org.dizitart.no2.IndexType;

public class NitriteDocumentStoreBuilder<Key, Doc extends Document> implements StoreBuilder<
        NitriteDocumentStore<Key, Doc>> {

    private final String name;
    private final Serde<Key> keySerde;
    private final DocumentSerde<Doc> valueSerde;
    private final String keyFieldName;
    private final Function<Document, Doc> documentConverter;

    private Map<String, String> logConfig = new HashMap<>();
    private final Map<String, IndexOptions> indices = new HashMap<>();
    private final List<NitriteCustomizer> customizers = new ArrayList<>();

    boolean enableLogging = true;

    public NitriteDocumentStoreBuilder(String name, String keyFieldName, Serde<Key> keySerde, Class<Doc> docClass, ObjectMapper objectMapper) {
        this.name = name;
        this.keySerde = keySerde;
        this.valueSerde = new DocumentSerde<>(docClass, objectMapper);
        this.keyFieldName = keyFieldName;
        this.documentConverter = (document) -> objectMapper.convertValue(document, docClass);
    }

    @Override
    public NitriteDocumentStoreBuilder<Key, Doc> withCachingEnabled() {
        throw new UnsupportedOperationException("caching is not available for nitrite stores");
    }

    @Override
    public NitriteDocumentStoreBuilder<Key, Doc> withCachingDisabled() {
        return this;
    }

    @Override
    public NitriteDocumentStoreBuilder<Key, Doc> withLoggingEnabled(Map<String, String> config) {
        Objects.requireNonNull(config, "config can't be null");
        enableLogging = true;
        this.logConfig = config;
        return this;
    }

    @Override
    public NitriteDocumentStoreBuilder<Key, Doc> withLoggingDisabled() {
        enableLogging = false;
        logConfig.clear();
        return this;
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
    public NitriteDocumentStoreBuilder<Key, Doc> withIndexOn(String field) {
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
    public NitriteDocumentStoreBuilder<Key, Doc> withUniqueIndexOn(String field) {
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
    public NitriteDocumentStoreBuilder<Key, Doc> withFullTextIndexOn(String field) {
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
    public NitriteDocumentStoreBuilder<Key, Doc> withIndex(String field, IndexOptions options) {
        indices.put(field, options);
        return this;
    }

    /**
     * Adds a customizer to customize the Nitrite document store.
     *
     * @param customizer the customizer
     * @return the Nitrite document store builder
     */
    public NitriteDocumentStoreBuilder<Key, Doc> withCustomizer(NitriteCustomizer customizer) {
        customizers.add(customizer);
        return this;
    }

    @Override
    public NitriteDocumentStore<Key, Doc> build() {
        return new NitriteDocumentStore(this.name, this.keySerde, this.valueSerde, this.keyFieldName, this.documentConverter, indices, customizers, enableLogging);
    }

    @Override
    public Map<String, String> logConfig() {
        return logConfig;
    }

    @Override
    public boolean loggingEnabled() {
        return enableLogging;
    }

    @Override
    public String name() {
        return this.name;
    }
}
