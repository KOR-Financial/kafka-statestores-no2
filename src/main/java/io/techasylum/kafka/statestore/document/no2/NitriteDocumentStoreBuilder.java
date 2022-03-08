package io.techasylum.kafka.statestore.document.no2;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.state.StoreBuilder;
import org.dizitart.no2.Document;

import java.util.Map;

public class NitriteDocumentStoreBuilder<K> implements StoreBuilder<NitriteDocumentStore<K>> {
    private final String name;
    private final Serde<K> keySerde;
    private final Serde<Document> valueSerde;
    private final String keyFieldName;

    private Map<String, String> logConfig;

    public NitriteDocumentStoreBuilder(String name, Serde<K> keySerde, Serde<Document> valueSerde, String keyFieldName) {
        this.name = name;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.keyFieldName = keyFieldName;
    }

    @Override
    public StoreBuilder<NitriteDocumentStore<K>> withCachingEnabled() {
        throw new UnsupportedOperationException("caching is not available for nitrite stores");
    }

    @Override
    public StoreBuilder<NitriteDocumentStore<K>> withCachingDisabled() {
        throw new UnsupportedOperationException("caching is not available for nitrite stores");
    }

    @Override
    public StoreBuilder<NitriteDocumentStore<K>> withLoggingEnabled(Map<String, String> config) {
        this.logConfig = config;
        return this;
    }

    @Override
    public StoreBuilder<NitriteDocumentStore<K>> withLoggingDisabled() {
        throw new UnsupportedOperationException("logging cannot be turned off for nitrite stores");
    }

    @Override
    public NitriteDocumentStore<K> build() {
        return new NitriteDocumentStore(this.name, this.keySerde, this.valueSerde, this.keyFieldName);
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
