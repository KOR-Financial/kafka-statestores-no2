package io.techasylum.kafka.statestore.document;

import io.techasylum.kafka.statestore.document.no2.NitriteDocumentStoreBuilder;
import org.apache.kafka.common.serialization.Serde;
import org.dizitart.no2.Document;

public final class DocumentStores {

    public static <K> NitriteDocumentStoreBuilder<K> nitriteStore(String name, Serde<K> keySerde, Serde<Document> valueSerde, String keyFieldName) {
        return new NitriteDocumentStoreBuilder<>(name, keySerde, valueSerde, keyFieldName);
    }

}
