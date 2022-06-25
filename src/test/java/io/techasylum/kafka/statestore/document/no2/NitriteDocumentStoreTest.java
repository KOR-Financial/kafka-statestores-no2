package io.techasylum.kafka.statestore.document.no2;

import java.io.File;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.techasylum.kafka.statestore.document.internals.InternalMockProcessorContext;
import io.techasylum.kafka.statestore.document.serialization.DocumentSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.dizitart.no2.Document;
import org.junit.jupiter.api.Test;

import static java.util.Collections.emptyList;
import static org.dizitart.no2.IndexOptions.indexOptions;
import static org.dizitart.no2.IndexType.Fulltext;

class NitriteDocumentStoreTest {

    private final ObjectMapper mapper = new ObjectMapper();

    @Test
    void shouldInitCorrectly() {
        NitriteDocumentStore<String, Document> store = new NitriteDocumentStore("name", Serdes.String(), new DocumentSerde<>(Document.class, mapper), "testField", (document) -> document, Map.of("testField", indexOptions(Fulltext)), emptyList(), true);
        String dir = String.format("%s/NitriteDocumentStoreTest/%s",
                System.getProperty("java.io.tmpdir"), UUID.randomUUID());
        InternalProcessorContext ctx = new InternalMockProcessorContext(new File(dir), new StreamsConfig(Map.of("application.id", "test", "bootstrap.servers", "mock://mock.com")));
        store.init((StateStoreContext) ctx, store);
    }

    @Test
    void shouldNotCreateIndicesThatAlreadyExist() {
        NitriteDocumentStore<String, Document> store = new NitriteDocumentStore("name", Serdes.String(), new DocumentSerde<>(Document.class, mapper), "testField", (document) -> document, Map.of("testField", indexOptions(Fulltext)), emptyList(), true);
        String dir = String.format("%s/NitriteDocumentStoreTest/%s",
                System.getProperty("java.io.tmpdir"), UUID.randomUUID());
        InternalProcessorContext ctx = new InternalMockProcessorContext(new File(dir), new StreamsConfig(Map.of("application.id", "test", "bootstrap.servers", "mock://mock.com")));
        store.init((StateStoreContext) ctx, store);
        store.close();
        store.init((StateStoreContext) ctx, store);
    }

}