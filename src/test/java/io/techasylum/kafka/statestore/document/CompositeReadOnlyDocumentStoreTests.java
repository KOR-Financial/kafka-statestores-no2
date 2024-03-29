package io.techasylum.kafka.statestore.document;

import java.io.File;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.techasylum.kafka.statestore.document.composite.CompositeCursor;
import io.techasylum.kafka.statestore.document.composite.CompositeFindOptions;
import io.techasylum.kafka.statestore.document.composite.CompositeIndexedDocumentStore;
import io.techasylum.kafka.statestore.document.composite.CompositeReadOnlyDocumentStore;
import io.techasylum.kafka.statestore.document.internals.InternalMockProcessorContext;
import io.techasylum.kafka.statestore.document.internals.MockRecordCollector;
import io.techasylum.kafka.statestore.document.internals.StateStoreProviderStub;
import io.techasylum.kafka.statestore.document.internals.WrappingStoreProvider;
import io.techasylum.kafka.statestore.document.no2.NitriteDocumentStore;
import io.techasylum.kafka.statestore.document.no2.movies.Movie;
import io.techasylum.kafka.statestore.document.serialization.DocumentSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.internals.CompositeReadOnlyKeyValueStore;
import org.dizitart.no2.Document;
import org.dizitart.no2.Index;
import org.dizitart.no2.exceptions.FilterException;
import org.dizitart.no2.exceptions.IndexingException;
import org.dizitart.no2.exceptions.ValidationException;
import org.dizitart.no2.filters.Filters;
import org.dizitart.no2.filters.PatchedFilters;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;
import static org.dizitart.no2.IndexOptions.indexOptions;
import static org.dizitart.no2.IndexType.Fulltext;
import static org.dizitart.no2.IndexType.Unique;
import static org.dizitart.no2.SortOrder.Ascending;
import static org.dizitart.no2.SortOrder.Descending;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CompositeReadOnlyDocumentStoreTests {

    private final String storeName = "my-store";
    private StateStoreProviderStub stubProviderTwo;
    private NitriteDocumentStore<String, Document> stubOneUnderlying;
    private NitriteDocumentStore<String, Document> otherUnderlyingStore;
    private CompositeReadOnlyDocumentStore<String, Document> theStore;
    private CompositeIndexedDocumentStore theIndexedStore;
    private DocumentSerde<Document> movieSerde;

    private ObjectMapper objectMapper;

    private final Movie speed = new Movie("SPEED", "Speed", 1994, 7.3f);
    private final Movie matrix1 = new Movie("MTRX1", "The Matrix", 1999, 8.7f);
    private final Movie matrix2 = new Movie("MTRX2", "The Matrix Reloaded", 2003, 7.2f);
    private final Movie matrix3 = new Movie("MTRX3", "The Matrix Revolutions", 2003, 6.8f);
    private final Movie matrix4 = new Movie("MTRX4", "The Matrix Resurrections", 2021, 6.0f);

    @BeforeEach
    public void before() {
        objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
        movieSerde = new DocumentSerde<>(Document.class, objectMapper);

        final StateStoreProviderStub stubProviderOne = new StateStoreProviderStub(false);
        stubProviderTwo = new StateStoreProviderStub(false);

        stubOneUnderlying = newStoreInstance(0);
        stubProviderOne.addStore(storeName, 2, stubOneUnderlying);
        otherUnderlyingStore = newStoreInstance(1);
        stubProviderOne.addStore("other-store", 3, otherUnderlyingStore);
        theStore = new CompositeReadOnlyDocumentStore<>(
                new WrappingStoreProvider(asList(stubProviderOne, stubProviderTwo), StoreQueryParameters.fromNameAndType(storeName, new QueryableDocumentStoreTypes.DocumentStoreType<>())),
                new QueryableDocumentStoreTypes.DocumentStoreType<>(),
                storeName
        );
        theIndexedStore = new CompositeIndexedDocumentStore(
                new WrappingStoreProvider(asList(stubProviderOne, stubProviderTwo), StoreQueryParameters.fromNameAndType(storeName, new QueryableDocumentStoreTypes.IndexedDocumentStoreType())),
                new QueryableDocumentStoreTypes.IndexedDocumentStoreType(),
                storeName
        );
    }

    private NitriteDocumentStore<String, Document> newStoreInstance(int partition) {
        final NitriteDocumentStore<String, Document> store = DocumentStores.nitriteStore(storeName, "code", Serdes.String(), Document.class, objectMapper).build();
        File storeDir = getNewStoreDir();

        @SuppressWarnings("rawtypes") final InternalMockProcessorContext context =
                new InternalMockProcessorContext<>(
                        partition,
                        storeDir,
                        new StateSerdes<>(
                                ProcessorStateManager.storeChangelogTopic("appId", storeName),
                                Serdes.String(),
                                Serdes.String()
                        ),
                        new MockRecordCollector()
                );
        context.setTime(1L);

        store.init((StateStoreContext) context, store);

        return store;
    }

    @Test
    public void shouldReturnNullIfKeyDoesNotExist() {
        assertNull(theStore.get("whatever"));
    }

    @Test
    public void shouldThrowNullPointerExceptionOnGetNullKey() {
        assertThrows(NullPointerException.class, () -> theStore.get(null));
    }

    @Test
    public void shouldThrowNullPointerExceptionOnFindNullFromKey() {
        assertThrows(NullPointerException.class, () -> theStore.find(null));
    }

    @Test
    public void shouldReturnValueIfExists() {
        stubOneUnderlying.put(speed.code(), new Document(objectMapper.convertValue(speed, HashMap.class)));
        assertEquals(speed, objectMapper.convertValue(theStore.get(speed.code()), Movie.class));
    }

    @Test
    public void shouldNotGetValuesFromOtherStores() {
        otherUnderlyingStore.put(matrix1.code(), new Document(objectMapper.convertValue(matrix1, HashMap.class)));
        assertNull(theStore.get(matrix1.code()));
    }

    @Test
    public void shouldFindValueWithFieldQuery() {
        stubOneUnderlying.put(matrix2.code(), new Document(objectMapper.convertValue(matrix2, HashMap.class)));
        final CompositeCursor<Document> queryCursor = theStore.find(PatchedFilters.eq("title", matrix2.title()));
        assertThat(objectMapper.convertValue(queryCursor.firstOrDefault(), Movie.class)).isEqualTo(matrix2);
    }

    @Test
    public void shouldFindValueForKeyWhenMultiStores() {
        final WritableDocumentStore<String, Document> store = newStoreInstance(3);
        stubProviderTwo.addStore(storeName, store);

        store.put(matrix1.code(), new Document(objectMapper.convertValue(matrix1, HashMap.class)));
        stubOneUnderlying.put(matrix2.code(), new Document(objectMapper.convertValue(matrix2, HashMap.class)));

        assertEquals(matrix1, objectMapper.convertValue(theStore.get(matrix1.code()), Movie.class));
        assertEquals(matrix2, objectMapper.convertValue(theStore.get(matrix2.code()), Movie.class));
    }

    @Test
    public void shouldFindMultipleValues() {
        stubOneUnderlying.put(matrix1.code(), new Document(objectMapper.convertValue(matrix1, HashMap.class)));
        stubOneUnderlying.put(matrix2.code(), new Document(objectMapper.convertValue(matrix2, HashMap.class)));
        stubOneUnderlying.put(matrix3.code(), new Document(objectMapper.convertValue(matrix3, HashMap.class)));

        CompositeCursor<Document> movieQueryCursor = theStore.find(PatchedFilters.gt("year", 2000));
        assertThat(movieQueryCursor.totalCount()).isEqualTo(2);
        assertThat(movieQueryCursor.size()).isEqualTo(2);
        assertThat(movieQueryCursor.toList()).hasSize(2);
        assertThat(movieQueryCursor.toList()).map((d) -> d.get("code")).containsExactlyInAnyOrder(matrix2.code(), matrix3.code());
    }

    @Test
    public void shouldSupportPagination() {
        stubOneUnderlying.put(matrix1.code(), new Document(objectMapper.convertValue(matrix1, HashMap.class)));
        stubOneUnderlying.put(matrix2.code(), new Document(objectMapper.convertValue(matrix2, HashMap.class)));
        stubOneUnderlying.put(matrix3.code(), new Document(objectMapper.convertValue(matrix3, HashMap.class)));

        CompositeCursor<Document> movieQueryCursor1 = theStore.findWithOptions(CompositeFindOptions.limit(Map.of(0, 0),2));
        assertThat(movieQueryCursor1.totalCount()).isEqualTo(3);
        assertThat(movieQueryCursor1.size()).isEqualTo(2);
        assertThat(movieQueryCursor1.toList()).hasSize(2);
        assertThat(movieQueryCursor1.hasMore()).isTrue();
        Map<Integer, Integer> nextOffsets = ((CompositeCursor) movieQueryCursor1).nextOffsets();
        assertThat(nextOffsets).containsExactlyEntriesOf(Map.of(0, 2));
        CompositeCursor<Document> movieQueryCursor2 = theStore.findWithOptions(CompositeFindOptions.limit(nextOffsets,2));
        assertThat(movieQueryCursor2.totalCount()).isEqualTo(3);
        assertThat(movieQueryCursor2.size()).isEqualTo(1);
        assertThat(movieQueryCursor2.toList()).hasSize(1);
        assertThat(movieQueryCursor2.hasMore()).isFalse();
    }

    @Test
    public void shouldSupportPaginationWhileFiltering() {
        stubOneUnderlying.put(matrix1.code(), new Document(objectMapper.convertValue(matrix1, HashMap.class)));
        stubOneUnderlying.put(matrix2.code(), new Document(objectMapper.convertValue(matrix2, HashMap.class)));
        stubOneUnderlying.put(matrix3.code(), new Document(objectMapper.convertValue(matrix3, HashMap.class)));

        CompositeCursor<Document> movieQueryCursor1 = theStore.findWithOptions(PatchedFilters.gt("year", 2000), CompositeFindOptions.limit(Map.of(0, 0),1));
        assertThat(movieQueryCursor1.totalCount()).isEqualTo(2);
        assertThat(movieQueryCursor1.size()).isEqualTo(1);
        assertThat(movieQueryCursor1.toList()).hasSize(1);
        assertThat(movieQueryCursor1.hasMore()).isTrue();
        Map<Integer, Integer> nextOffsets = ((CompositeCursor) movieQueryCursor1).nextOffsets();
        assertThat(nextOffsets).containsExactlyEntriesOf(Map.of(0, 1));
        CompositeCursor<Document> movieQueryCursor2 = theStore.findWithOptions(PatchedFilters.gt("year", 2000), CompositeFindOptions.limit(nextOffsets,1));
        assertThat(movieQueryCursor2.totalCount()).isEqualTo(2);
        assertThat(movieQueryCursor2.size()).isEqualTo(1);
        assertThat(movieQueryCursor2.toList()).hasSize(1);
        assertThat(movieQueryCursor2.hasMore()).isFalse();
    }

    @Test
    public void shouldSupportFindAcrossMultipleStores() {
        final WritableDocumentStore<String, Document> store = newStoreInstance(3);
        stubProviderTwo.addStore(storeName, store);

        stubOneUnderlying.put(matrix1.code(), new Document(objectMapper.convertValue(matrix1, HashMap.class)));
        stubOneUnderlying.put(matrix2.code(), new Document(objectMapper.convertValue(matrix2, HashMap.class)));

        store.put(matrix3.code(), new Document(objectMapper.convertValue(matrix3, HashMap.class)));
        store.put(speed.code(), new Document(objectMapper.convertValue(speed, HashMap.class)));

        CompositeCursor<Document> movieQueryCursor = theStore.findWithOptions(PatchedFilters.gt("year", 2000), CompositeFindOptions.sort("year", Ascending));
        assertThat(movieQueryCursor.toList()).map((d) -> d.get("code")).containsExactlyInAnyOrder(matrix2.code(), matrix3.code());
    }

    @Test
    public void shouldSupportLimitAcrossMultipleStores() {
        final WritableDocumentStore<String, Document> store = newStoreInstance(1);
        stubProviderTwo.addStore(storeName, store);

        stubOneUnderlying.put(matrix1.code(), new Document(objectMapper.convertValue(matrix1, HashMap.class)));
        stubOneUnderlying.put(matrix2.code(), new Document(objectMapper.convertValue(matrix2, HashMap.class)));

        store.put(matrix3.code(), new Document(objectMapper.convertValue(matrix3, HashMap.class)));
        store.put(matrix4.code(), new Document(objectMapper.convertValue(matrix4, HashMap.class)));
        store.put(speed.code(), new Document(objectMapper.convertValue(speed, HashMap.class)));

        CompositeCursor<Document> movieQueryCursorPage1 = theStore.findWithOptions(CompositeFindOptions.sort("year", Ascending).thenLimit(Map.of(0, 0, 1, 0), 2));
        assertThat(movieQueryCursorPage1.toList()).map((d) -> d.get("code")).containsExactly(speed.code(), matrix1.code());
        assertThat(movieQueryCursorPage1.hasMore()).isTrue();
        CompositeCursor<Document> movieQueryCursorPage2 = theStore.findWithOptions(CompositeFindOptions.sort("year", Ascending).thenLimit(((CompositeCursor) movieQueryCursorPage1).nextOffsets(), 2));
        assertThat(movieQueryCursorPage2.toList()).map((d) -> d.get("code")).containsExactlyInAnyOrder(matrix2.code(), matrix3.code());
        assertThat(movieQueryCursorPage2.hasMore()).isTrue();
        CompositeCursor<Document> movieQueryCursorPage3 = theStore.findWithOptions(CompositeFindOptions.sort("year", Ascending).thenLimit(((CompositeCursor) movieQueryCursorPage2).nextOffsets(), 2));
        assertThat(movieQueryCursorPage3.toList()).map((d) -> d.get("code")).containsExactly(matrix4.code());
        assertThat(movieQueryCursorPage3.hasMore()).isFalse();
    }

    @Test
    public void shouldSupportLimitAcrossMultipleStoresWithAllPageSizes() {
        final WritableDocumentStore<String, Document> store = newStoreInstance(1);
        stubProviderTwo.addStore(storeName, store);

        stubOneUnderlying.put(matrix1.code(), new Document(objectMapper.convertValue(matrix1, HashMap.class)));
        stubOneUnderlying.put(matrix2.code(), new Document(objectMapper.convertValue(matrix2, HashMap.class)));

        store.put(matrix3.code(), new Document(objectMapper.convertValue(matrix3, HashMap.class)));
        store.put(matrix4.code(), new Document(objectMapper.convertValue(matrix4, HashMap.class)));
        store.put(speed.code(), new Document(objectMapper.convertValue(speed, HashMap.class)));

        CompositeCursor<Document> movieQueryCursorPageSize1 = theStore.findWithOptions(CompositeFindOptions.sort("rating", Descending).thenLimit(1));
        assertThat(movieQueryCursorPageSize1.toList()).map((d) -> d.get("code")).containsExactly(matrix1.code());
        assertThat(movieQueryCursorPageSize1.hasMore()).isTrue();

        CompositeCursor<Document> movieQueryCursorPageSize2 = theStore.findWithOptions(CompositeFindOptions.sort("rating", Descending).thenLimit(2));
        assertThat(movieQueryCursorPageSize2.toList()).map((d) -> d.get("code")).containsExactly(matrix1.code(), speed.code());
        assertThat(movieQueryCursorPageSize2.hasMore()).isTrue();

        CompositeCursor<Document> movieQueryCursorPageSize3 = theStore.findWithOptions(CompositeFindOptions.sort("rating", Descending).thenLimit(3));
        assertThat(movieQueryCursorPageSize3.toList()).map((d) -> d.get("code")).containsExactly(matrix1.code(), speed.code(), matrix2.code());
        assertThat(movieQueryCursorPageSize3.hasMore()).isTrue();

        CompositeCursor<Document> movieQueryCursorPageSize4 = theStore.findWithOptions(CompositeFindOptions.sort("rating", Descending).thenLimit(4));
        assertThat(movieQueryCursorPageSize4.toList()).map((d) -> d.get("code")).containsExactly(matrix1.code(), speed.code(), matrix2.code(), matrix3.code());
        assertThat(movieQueryCursorPageSize4.hasMore()).isTrue();

        CompositeCursor<Document> movieQueryCursorPageSize5 = theStore.findWithOptions(CompositeFindOptions.sort("rating", Descending).thenLimit(5));
        assertThat(movieQueryCursorPageSize5.toList()).map((d) -> d.get("code")).containsExactly(matrix1.code(), speed.code(), matrix2.code(), matrix3.code(), matrix4.code());
        assertThat(movieQueryCursorPageSize5.hasMore()).isFalse();

        CompositeCursor<Document> movieQueryCursorPage1 = theStore.findWithOptions(CompositeFindOptions.sort("rating", Descending).thenLimit(2));
        assertThat(movieQueryCursorPage1.toList()).map((d) -> d.get("code")).containsExactly(matrix1.code(), speed.code());
        assertThat(movieQueryCursorPage1.hasMore()).isTrue();

        CompositeCursor<Document> movieQueryCursorPage2 = theStore.findWithOptions(CompositeFindOptions.sort("rating", Descending).thenLimit(((CompositeCursor) movieQueryCursorPage1).nextOffsets(), 2));
        assertThat(movieQueryCursorPage2.toList()).map((d) -> d.get("code")).containsExactly(matrix2.code(), matrix3.code());
        assertThat(movieQueryCursorPage2.hasMore()).isTrue();

        CompositeCursor<Document> movieQueryCursorPage3 = theStore.findWithOptions(CompositeFindOptions.sort("rating", Descending).thenLimit(((CompositeCursor) movieQueryCursorPage2).nextOffsets(), 2));
        assertThat(movieQueryCursorPage3.toList()).map((d) -> d.get("code")).containsExactly(matrix4.code());
        assertThat(movieQueryCursorPage3.hasMore()).isFalse();
    }

    // TODO: we don't expect partitions to change, and if they do pagination should be reset either way, so should we be more strict on this?
    @Test
    public void shouldSupportOffsetsForUnknownPartitions() {
        final WritableDocumentStore<String, Document> store = newStoreInstance(1);
        stubProviderTwo.addStore(storeName, store);

        CompositeCursor<Document> movieQueryCursorPage1 = theStore.findWithOptions(CompositeFindOptions.sort("rating", Descending).thenLimit(2));
        assertThat(movieQueryCursorPage1.toList()).isEmpty();
        assertThat(movieQueryCursorPage1.hasMore()).isFalse();

        CompositeCursor<Document> movieQueryCursorPage2 = theStore.findWithOptions(CompositeFindOptions.sort("rating", Descending).thenLimit(Map.of(0, 0, 1, 0, 2, 1, 3, 5), 2));
        assertThat(movieQueryCursorPage2.toList()).isEmpty();
        assertThat(movieQueryCursorPage2.hasMore()).isFalse();
    }

    // TODO: what if the state store shrinks as we paginate through it, due to retention policies or tombstone records?
    @Test
    public void shouldThrowExceptionOnInvalidOffsetsForExistingPartitions() {
        final WritableDocumentStore<String, Document> store = newStoreInstance(1);
        stubProviderTwo.addStore(storeName, store);

        assertThrows(ValidationException.class, () -> theStore.findWithOptions(CompositeFindOptions.sort("rating", Descending).thenLimit(Map.of(0, 999, 1, 0), 2)));
    }

    @Test
    public void shouldSupportLimitAcrossMultipleStoresWithAllElementsOfFirstPageInSamePartition() {
        final WritableDocumentStore<String, Document> store = newStoreInstance(1);
        stubProviderTwo.addStore(storeName, store);

        stubOneUnderlying.put(matrix1.code(), new Document(objectMapper.convertValue(matrix1, HashMap.class)));
        stubOneUnderlying.put(speed.code(), new Document(objectMapper.convertValue(speed, HashMap.class)));

        store.put(matrix2.code(), new Document(objectMapper.convertValue(matrix2, HashMap.class)));
        store.put(matrix3.code(), new Document(objectMapper.convertValue(matrix3, HashMap.class)));
        store.put(matrix4.code(), new Document(objectMapper.convertValue(matrix4, HashMap.class)));

        CompositeCursor<Document> movieQueryCursorPage1 = theStore.findWithOptions(CompositeFindOptions.sort("rating", Descending).thenLimit(2));
        assertThat(movieQueryCursorPage1.toList()).map((d) -> d.get("code")).containsExactly(matrix1.code(), speed.code());
        assertThat(movieQueryCursorPage1.hasMore()).isTrue();

        CompositeCursor<Document> movieQueryCursorPage2 = theStore.findWithOptions(CompositeFindOptions.sort("rating", Descending).thenLimit(((CompositeCursor) movieQueryCursorPage1).nextOffsets(), 2));
        assertThat(movieQueryCursorPage2.toList()).map((d) -> d.get("code")).containsExactly(matrix2.code(), matrix3.code());
        assertThat(movieQueryCursorPage2.hasMore()).isTrue();

        CompositeCursor<Document> movieQueryCursorPage3 = theStore.findWithOptions(CompositeFindOptions.sort("rating", Descending).thenLimit(((CompositeCursor) movieQueryCursorPage2).nextOffsets(), 2));
        assertThat(movieQueryCursorPage3.toList()).map((d) -> d.get("code")).containsExactly(matrix4.code());
        assertThat(movieQueryCursorPage3.hasMore()).isFalse();
    }

    @Test
    public void shouldFailTextFilterOnNonIndexedField() {
        stubOneUnderlying.put(matrix1.code(), new Document(objectMapper.convertValue(matrix1, HashMap.class)));
        stubOneUnderlying.put(matrix2.code(), new Document(objectMapper.convertValue(matrix2, HashMap.class)));
        stubOneUnderlying.put(matrix3.code(), new Document(objectMapper.convertValue(matrix3, HashMap.class)));

        FilterException filterException = assertThrows(FilterException.class, () -> theStore.find(Filters.text("title", "The Matrix")));
        assertThat(filterException).hasCauseExactlyInstanceOf(IndexingException.class);
        assertThat(filterException.getCause()).hasMessage("NO2.5001: title is not indexed");
    }

    @Test
    public void shouldSupportTextFilterOnIndex() {
        stubOneUnderlying.put(speed.code(), new Document(objectMapper.convertValue(speed, HashMap.class)));
        stubOneUnderlying.put(matrix1.code(), new Document(objectMapper.convertValue(matrix1, HashMap.class)));
        stubOneUnderlying.put(matrix2.code(), new Document(objectMapper.convertValue(matrix2, HashMap.class)));
        stubOneUnderlying.put(matrix3.code(), new Document(objectMapper.convertValue(matrix3, HashMap.class)));

        theIndexedStore.createIndex("title", indexOptions(Fulltext, false));
        CompositeCursor<Document> movieQueryCursor = theStore.find(Filters.text("title", "The Matrix"));
        assertThat(movieQueryCursor.totalCount()).isEqualTo(3);
        assertThat(movieQueryCursor.size()).isEqualTo(3);
        assertThat(movieQueryCursor.toList()).hasSize(3);
        assertThat(movieQueryCursor.toList()).map((d) -> d.get("code")).containsExactlyInAnyOrder(matrix1.code(), matrix2.code(), matrix3.code());
    }

    @Test
    public void shouldFailIndexingFieldTwice() {
        theIndexedStore.createIndex("title", indexOptions(Fulltext));
        IndexingException indexingException = assertThrows(IndexingException.class, () -> theIndexedStore.createIndex("title", indexOptions(Fulltext)));
        assertThat(indexingException).hasMessage("NO2.5005: index already exists on title");
    }

    @Test
    public void shouldFailDroppingIndexFieldTwice() {
        theIndexedStore.createIndex("title", indexOptions(Fulltext));
        theIndexedStore.dropIndex("title");
        IndexingException indexingException = assertThrows(IndexingException.class, () -> theIndexedStore.dropIndex("title"));
        assertThat(indexingException).hasMessage("NO2.5010: title is not indexed");
    }

    @Test
    public void shouldIndexField() {
        stubOneUnderlying.put(speed.code(), new Document(objectMapper.convertValue(speed, HashMap.class)));
        stubOneUnderlying.put(matrix1.code(), new Document(objectMapper.convertValue(matrix1, HashMap.class)));
        stubOneUnderlying.put(matrix2.code(), new Document(objectMapper.convertValue(matrix2, HashMap.class)));
        stubOneUnderlying.put(matrix3.code(), new Document(objectMapper.convertValue(matrix3, HashMap.class)));

        assertThat(theIndexedStore.hasIndex("title")).extractingFromEntries(Map.Entry::getValue).containsOnly(false);
        theIndexedStore.createIndex("title", indexOptions(Fulltext, true));

        await().atMost(5, SECONDS).untilAsserted(() -> {
            Map<Integer, Boolean> indexingPartitions = theIndexedStore.isIndexing("title");
            assertThat(indexingPartitions).hasSize(1);
            assertThat(indexingPartitions.get(0)).isFalse();
        });

        Map<Integer, Collection<Index>> retrievedIndices = theIndexedStore.listIndices();
        assertThat(retrievedIndices).hasSize(1);
        assertThat(retrievedIndices.get(0)).hasSize(1);
        assertThat(retrievedIndices.get(0)).containsOnlyOnce(new Index(Fulltext, "title", "my-store"));
    }

    @Test
    public void shouldDropIndex() {
        stubOneUnderlying.put(speed.code(), new Document(objectMapper.convertValue(speed, HashMap.class)));
        stubOneUnderlying.put(matrix1.code(), new Document(objectMapper.convertValue(matrix1, HashMap.class)));
        stubOneUnderlying.put(matrix2.code(), new Document(objectMapper.convertValue(matrix2, HashMap.class)));
        stubOneUnderlying.put(matrix3.code(), new Document(objectMapper.convertValue(matrix3, HashMap.class)));

        theIndexedStore.createIndex("title", indexOptions(Fulltext, false));
        assertThat(theIndexedStore.hasIndex("title")).extractingFromEntries(Map.Entry::getValue).containsOnly(true);

        theIndexedStore.dropIndex("title");
        assertThat(theIndexedStore.hasIndex("title")).extractingFromEntries(Map.Entry::getValue).containsOnly(false);

        Map<Integer, Boolean> indexingPartitions = theIndexedStore.isIndexing("title");
        assertThat(indexingPartitions).hasSize(1);
        assertThat(indexingPartitions.get(0)).isFalse();

        Map<Integer, Collection<Index>> retrievedIndices = theIndexedStore.listIndices();
        assertThat(retrievedIndices).hasSize(1);
        assertThat(retrievedIndices.get(0)).isEmpty();
    }

    @Test
    public void shouldDropAllIndices() {
        stubOneUnderlying.put(speed.code(), new Document(objectMapper.convertValue(speed, HashMap.class)));
        stubOneUnderlying.put(matrix1.code(), new Document(objectMapper.convertValue(matrix1, HashMap.class)));
        stubOneUnderlying.put(matrix2.code(), new Document(objectMapper.convertValue(matrix2, HashMap.class)));
        stubOneUnderlying.put(matrix3.code(), new Document(objectMapper.convertValue(matrix3, HashMap.class)));

        theIndexedStore.createIndex("code", indexOptions(Unique, false));
        theIndexedStore.createIndex("title", indexOptions(Fulltext, false));
        assertThat(theIndexedStore.hasIndex("code")).extractingFromEntries(Map.Entry::getValue).containsOnly(true);
        assertThat(theIndexedStore.hasIndex("title")).extractingFromEntries(Map.Entry::getValue).containsOnly(true);

        theIndexedStore.dropAllIndices();
        assertThat(theIndexedStore.hasIndex("code")).extractingFromEntries(Map.Entry::getValue).containsOnly(false);
        assertThat(theIndexedStore.hasIndex("title")).extractingFromEntries(Map.Entry::getValue).containsOnly(false);

        Map<Integer, Boolean> indexingPartitions = theIndexedStore.isIndexing("title");
        assertThat(indexingPartitions).hasSize(1);
        assertThat(indexingPartitions.get(0)).isFalse();

        Map<Integer, Collection<Index>> retrievedIndices = theIndexedStore.listIndices();
        assertThat(retrievedIndices).hasSize(1);
        assertThat(retrievedIndices.get(0)).isEmpty();
    }

    /**
    @Test
    public void shouldSupportPrefixScanAcrossMultipleKVStores() {
        final DocumentStore<String, Movie, ObjectFilter, FindOptions> store = newStoreInstance();
        stubProviderTwo.addStore(storeName, store);

        stubOneUnderlying.put("a", "a");
        stubOneUnderlying.put("b", "b");
        stubOneUnderlying.put("z", "z");

        store.put("aa", "c");
        store.put("ab", "d");
        store.put("x", "x");

        final List<KeyValue<String, String>> results = toList(theStore.prefixScan("a", new StringSerializer()));
        assertArrayEquals(
                asList(
                        new KeyValue<>("a", "a"),
                        new KeyValue<>("aa", "c"),
                        new KeyValue<>("ab", "d")
                ).toArray(),
                results.toArray());
    }

    @Test
    public void shouldSupportReverseRangeAcrossMultipleKVStores() {
        final DocumentStore<String, Movie, ObjectFilter, FindOptions> store = newStoreInstance();
        stubProviderTwo.addStore(storeName, store);

        stubOneUnderlying.put("a", "a");
        stubOneUnderlying.put("b", "b");
        stubOneUnderlying.put("z", "z");

        store.put("c", "c");
        store.put("d", "d");
        store.put("x", "x");

        final List<KeyValue<String, String>> results = toList(theStore.reverseRange("a", "e"));
        assertTrue(results.contains(new KeyValue<>("a", "a")));
        assertTrue(results.contains(new KeyValue<>("b", "b")));
        assertTrue(results.contains(new KeyValue<>("c", "c")));
        assertTrue(results.contains(new KeyValue<>("d", "d")));
        assertEquals(4, results.size());
    }

    @Test
    public void shouldSupportAllAcrossMultipleStores() {
        final DocumentStore<String, Movie, ObjectFilter, FindOptions> store = newStoreInstance();
        stubProviderTwo.addStore(storeName, store);

        stubOneUnderlying.put("a", "a");
        stubOneUnderlying.put("b", "b");
        stubOneUnderlying.put("z", "z");

        store.put("c", "c");
        store.put("d", "d");
        store.put("x", "x");

        final List<KeyValue<String, String>> results = toList(theStore.all());
        assertTrue(results.contains(new KeyValue<>("a", "a")));
        assertTrue(results.contains(new KeyValue<>("b", "b")));
        assertTrue(results.contains(new KeyValue<>("c", "c")));
        assertTrue(results.contains(new KeyValue<>("d", "d")));
        assertTrue(results.contains(new KeyValue<>("x", "x")));
        assertTrue(results.contains(new KeyValue<>("z", "z")));
        assertEquals(6, results.size());
    }

    @Test
    public void shouldSupportReverseAllAcrossMultipleStores() {
        final DocumentStore<String, Movie, ObjectFilter, FindOptions> store = newStoreInstance();
        stubProviderTwo.addStore(storeName, store);

        stubOneUnderlying.put("a", "a");
        stubOneUnderlying.put("b", "b");
        stubOneUnderlying.put("z", "z");

        store.put("c", "c");
        store.put("d", "d");
        store.put("x", "x");

        final List<KeyValue<String, String>> results = toList(theStore.reverseAll());
        assertTrue(results.contains(new KeyValue<>("a", "a")));
        assertTrue(results.contains(new KeyValue<>("b", "b")));
        assertTrue(results.contains(new KeyValue<>("c", "c")));
        assertTrue(results.contains(new KeyValue<>("d", "d")));
        assertTrue(results.contains(new KeyValue<>("x", "x")));
        assertTrue(results.contains(new KeyValue<>("z", "z")));
        assertEquals(6, results.size());
    }

    @Test
    public void shouldThrowInvalidStoreExceptionDuringRebalance() {
        assertThrows(InvalidStateStoreException.class, () -> rebalancing().get("anything"));
    }

    @Test
    public void shouldThrowInvalidStoreExceptionOnApproximateNumEntriesDuringRebalance() {
        assertThrows(InvalidStateStoreException.class, () -> rebalancing().approximateNumEntries());
    }

    @Test
    public void shouldThrowInvalidStoreExceptionOnRangeDuringRebalance() {
        assertThrows(InvalidStateStoreException.class, () -> rebalancing().range("anything", "something"));
    }

    @Test
    public void shouldThrowInvalidStoreExceptionOnReverseRangeDuringRebalance() {
        assertThrows(InvalidStateStoreException.class, () -> rebalancing().reverseRange("anything", "something"));
    }

    @Test
    public void shouldThrowInvalidStoreExceptionOnPrefixScanDuringRebalance() {
        assertThrows(InvalidStateStoreException.class, () -> rebalancing().prefixScan("anything", new StringSerializer()));
    }

    @Test
    public void shouldThrowInvalidStoreExceptionOnAllDuringRebalance() {
        assertThrows(InvalidStateStoreException.class, () -> rebalancing().all());
    }

    @Test
    public void shouldThrowInvalidStoreExceptionOnReverseAllDuringRebalance() {
        assertThrows(InvalidStateStoreException.class, () -> rebalancing().reverseAll());
    }

    @Test
    public void shouldGetApproximateEntriesAcrossAllStores() {
        final DocumentStore<String, Movie, ObjectFilter, FindOptions> store = newStoreInstance();
        stubProviderTwo.addStore(storeName, store);

        stubOneUnderlying.put("a", "a");
        stubOneUnderlying.put("b", "b");
        stubOneUnderlying.put("z", "z");

        store.put("c", "c");
        store.put("d", "d");
        store.put("x", "x");

        assertEquals(6, theStore.approximateNumEntries());
    }

    @Test
    public void shouldReturnLongMaxValueOnOverflow() {
        stubProviderTwo.addStore(storeName, new NoOpReadOnlyStore<Object, Object>() {
            @Override
            public long approximateNumEntries() {
                return Long.MAX_VALUE;
            }
        });

        stubOneUnderlying.put("overflow", "me");
        assertEquals(Long.MAX_VALUE, theStore.approximateNumEntries());
    }

    @Test
    public void shouldReturnLongMaxValueOnUnderflow() {
        stubProviderTwo.addStore(storeName, new NoOpReadOnlyStore<Object, Object>() {
            @Override
            public long approximateNumEntries() {
                return Long.MAX_VALUE;
            }
        });
        stubProviderTwo.addStore("my-storeA", new NoOpReadOnlyStore<Object, Object>() {
            @Override
            public long approximateNumEntries() {
                return Long.MAX_VALUE;
            }
        });

        assertEquals(Long.MAX_VALUE, theStore.approximateNumEntries());
    }
    **/

    private CompositeReadOnlyKeyValueStore<Object, Object> rebalancing() {
        return new CompositeReadOnlyKeyValueStore<>(
                new WrappingStoreProvider(
                        singletonList(new StateStoreProviderStub(true)),
                        StoreQueryParameters.fromNameAndType(storeName, QueryableStoreTypes.keyValueStore())),
                QueryableStoreTypes.keyValueStore(),
                storeName
        );
    }

    @NotNull
    private File getNewStoreDir() {
        // Make sure to always have a new directory
        return new File(String.format("%s/%s/test-store-dir/%s", System.getProperty("java.io.tmpdir"), UUID.randomUUID(), storeName));
    }

}