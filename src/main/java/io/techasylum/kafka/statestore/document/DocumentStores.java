package io.techasylum.kafka.statestore.document;

import io.techasylum.kafka.statestore.document.no2.NitriteDocumentStoreBuilder;
import io.techasylum.kafka.statestore.document.serialization.DocumentSerde;
import org.apache.kafka.common.serialization.Serde;

public final class DocumentStores {

    public static <K> NitriteDocumentStoreBuilder<K> nitriteStore(String name, Serde<K> keySerde, DocumentSerde valueSerde, String keyFieldName) {
        return new NitriteDocumentStoreBuilder<>(name, keySerde, valueSerde, keyFieldName);
    }

}
