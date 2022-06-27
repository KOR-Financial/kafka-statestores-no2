package io.techasylum.kafka.statestore.document;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.techasylum.kafka.statestore.document.no2.NitriteDocumentStoreBuilder;
import org.apache.kafka.common.serialization.Serde;
import org.dizitart.no2.Document;

public final class DocumentStores {

    public static <Key, Doc extends Document> NitriteDocumentStoreBuilder<Key, Doc> nitriteStore(String name, String keyFieldName, Serde<Key> keySerde, Class<Doc> docClass, ObjectMapper objectMapper) {
        return new NitriteDocumentStoreBuilder<>(name, keyFieldName, keySerde, docClass, objectMapper);
    }

}
