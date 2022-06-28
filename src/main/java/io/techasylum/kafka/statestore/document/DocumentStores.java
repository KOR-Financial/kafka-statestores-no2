package io.techasylum.kafka.statestore.document;

import java.util.function.Function;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.techasylum.kafka.statestore.document.no2.NitriteDocumentStoreBuilder;
import io.techasylum.kafka.statestore.document.serialization.DocumentSerde;
import org.apache.kafka.common.serialization.Serde;
import org.dizitart.no2.Document;

public final class DocumentStores {

    public static <Key, Doc extends Document> NitriteDocumentStoreBuilder<Key, Doc> nitriteStore(String name, String keyFieldName, Serde<Key> keySerde, Class<Doc> docClass, ObjectMapper objectMapper) {
        return nitriteStore(name, keyFieldName, keySerde, new DocumentSerde<>(docClass, objectMapper), docClass, objectMapper);
    }

    public static <Key, Doc extends Document> NitriteDocumentStoreBuilder<Key, Doc> nitriteStore(String name, String keyFieldName, Serde<Key> keySerde, DocumentSerde<Doc> valueSerde, Class<Doc> docClass, ObjectMapper objectMapper) {
        return nitriteStore(name, keyFieldName, keySerde, valueSerde, (document) -> objectMapper.convertValue(document, docClass));
    }

    public static <Key, Doc extends Document> NitriteDocumentStoreBuilder<Key, Doc> nitriteStore(String name, String keyFieldName, Serde<Key> keySerde, DocumentSerde<Doc> valueSerde, Function<Document, Doc> converter) {
        return new NitriteDocumentStoreBuilder<>(name, keyFieldName, keySerde, valueSerde, converter);
    }

}
