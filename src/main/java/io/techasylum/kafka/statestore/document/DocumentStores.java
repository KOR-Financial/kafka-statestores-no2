package io.techasylum.kafka.statestore.document;

import io.techasylum.kafka.statestore.document.no2.NitriteDocumentStore;
import io.techasylum.kafka.statestore.document.no2.NitriteDocumentStoreBuilder;
import io.techasylum.kafka.statestore.document.no2.NitriteObjectStore;
import io.techasylum.kafka.statestore.document.no2.NitriteObjectStoreBuilder;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.state.StoreBuilder;

public final class DocumentStores {
    public static <K, V> StoreBuilder<NitriteObjectStore<K, V>> nitriteObjectStore(String name, Serde<K> keySerde, Serde<V> valueSerde, Class<V> valueClass, String keyFieldName) {
        return new NitriteObjectStoreBuilder<>(name, keySerde, valueSerde, valueClass, keyFieldName);
    }
    public static <K, V> StoreBuilder<NitriteDocumentStore<K>> nitriteStore(String name, Serde<K> keySerde, Serde<V> valueSerde, String keyFieldName) {
        return new NitriteDocumentStoreBuilder(name, keySerde, valueSerde, keyFieldName);
    }
}
