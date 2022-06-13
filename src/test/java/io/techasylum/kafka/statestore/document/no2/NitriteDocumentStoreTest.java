package io.techasylum.kafka.statestore.document.no2;

import java.io.File;
import java.util.Map;
import java.util.UUID;

import io.techasylum.kafka.statestore.document.internals.InternalMockProcessorContext;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.dizitart.no2.Document;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonSerde;

import static java.util.Collections.emptyList;
import static org.dizitart.no2.IndexOptions.indexOptions;
import static org.dizitart.no2.IndexType.Fulltext;

class NitriteDocumentStoreTest {

    @Test
    void shouldInitCorrectly() {
        NitriteDocumentStore store = new NitriteDocumentStore("name", Serdes.String(), new JsonSerde<Document>(), "testField", Map.of("testField", indexOptions(Fulltext)), emptyList(), true);
        String dir = String.format("%s/NitriteDocumentStoreTest/%s",
                System.getProperty("java.io.tmpdir"), UUID.randomUUID());
        InternalProcessorContext ctx = new InternalMockProcessorContext(new File(dir), new StreamsConfig(Map.of("application.id", "test", "bootstrap.servers", "mock://mock.com")));
        store.init((StateStoreContext) ctx, store);
    }

    @Test
    void shouldNotCreateIndicesThatAlreadyExist() {
        NitriteDocumentStore store = new NitriteDocumentStore("name", Serdes.String(), new JsonSerde<Document>(), "testField", Map.of("testField", indexOptions(Fulltext)), emptyList(), true);
        String dir = String.format("%s/NitriteDocumentStoreTest/%s",
                System.getProperty("java.io.tmpdir"), UUID.randomUUID());
        InternalProcessorContext ctx = new InternalMockProcessorContext(new File(dir), new StreamsConfig(Map.of("application.id", "test", "bootstrap.servers", "mock://mock.com")));
        store.init((StateStoreContext) ctx, store);
        store.close();
        store.init((StateStoreContext) ctx, store);
    }

}