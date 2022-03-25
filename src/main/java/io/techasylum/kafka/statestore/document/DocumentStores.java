package io.techasylum.kafka.statestore.document;

import io.techasylum.kafka.statestore.document.no2.NitriteDocumentStoreBuilder;
import io.techasylum.kafka.statestore.document.no2.NitriteObjectStore;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.state.StoreBuilder;
import org.dizitart.no2.Document;

public final class DocumentStores {

    public static <K> NitriteDocumentStoreBuilder<K> nitriteStore(String name, Serde<K> keySerde, Serde<Document> valueSerde, String keyFieldName) {
        return new NitriteDocumentStoreBuilder<>(name, keySerde, valueSerde, keyFieldName);
    }

    public static <K, V> StoreBuilder<NitriteObjectStore<K, V>> nitriteObjectStore(String name, Serde<K> keySerde, Serde<V> valueSerde, Class<V> valueClass, String keyFieldName) {
        throw new UnsupportedOperationException("Object store note yet supported");
//        return new NitriteObjectStoreBuilder<>(name, keySerde, valueSerde, valueClass, keyFieldName);
    }

}
