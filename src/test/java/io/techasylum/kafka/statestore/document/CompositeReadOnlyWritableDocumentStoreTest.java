package io.techasylum.kafka.statestore.document;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.techasylum.kafka.statestore.document.composite.CompositeCursor;
import io.techasylum.kafka.statestore.document.composite.CompositeFindOptions;
import io.techasylum.kafka.statestore.document.composite.CompositeReadOnlyDocumentStore;
import io.techasylum.kafka.statestore.document.internals.InternalMockProcessorContext;
import io.techasylum.kafka.statestore.document.internals.MockRecordCollector;
import io.techasylum.kafka.statestore.document.internals.StateStoreProviderStub;
import io.techasylum.kafka.statestore.document.internals.WrappingStoreProvider;
import io.techasylum.kafka.statestore.document.no2.NitriteDocumentStore;
import io.techasylum.kafka.statestore.document.no2.movies.Movie;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.internals.CompositeReadOnlyKeyValueStore;
import org.dizitart.no2.Cursor;
import org.dizitart.no2.Document;
import org.dizitart.no2.exceptions.ValidationException;
import org.dizitart.no2.filters.Filters;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonSerde;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.dizitart.no2.SortOrder.Ascending;
import static org.dizitart.no2.SortOrder.Descending;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class CompositeReadOnlyWritableDocumentStoreTest {

    private final String storeName = "my-store";
    private StateStoreProviderStub stubProviderTwo;
    private NitriteDocumentStore<String> stubOneUnderlying;
    private NitriteDocumentStore<String> otherUnderlyingStore;
    private CompositeReadOnlyDocumentStore<String> theStore;
    private final Serde<Document> movieSerde = new JsonSerde<>(Document.class);

    private ObjectMapper objectMapper;

    private final Movie speed = new Movie("SPEED", "Speed", 1994, 7.3f);
    private final Movie matrix1 = new Movie("MTRX1", "The Matrix", 1999, 8.7f);
    private final Movie matrix2 = new Movie("MTRX2", "The Matrix Reloaded", 2003, 7.2f);
    private final Movie matrix3 = new Movie("MTRX3", "The Matrix Revolutions", 2003, 6.8f);
    private final Movie matrix4 = new Movie("MTRX4", "The Matrix Resurrections", 2021, 6.0f);

    @BeforeEach
    public void before() throws IOException {
        objectMapper = new ObjectMapper();
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
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
    }

    private NitriteDocumentStore<String> newStoreInstance(int partition) throws IOException {
        final NitriteDocumentStore<String> store = DocumentStores.nitriteStore(storeName, Serdes.String(), movieSerde, "code").build();
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
        final Cursor queryCursor = theStore.find(Filters.eq("title", matrix2.title()));
        assertThat(objectMapper.convertValue(queryCursor.firstOrDefault(), Movie.class)).isEqualTo(matrix2);
    }

    @Test
    public void shouldFindValueForKeyWhenMultiStores() throws IOException {
        final WritableDocumentStore<String> store = newStoreInstance(3);
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

        Cursor movieQueryCursor = theStore.find(Filters.gt("year", 2000));
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

        Cursor movieQueryCursor1 = theStore.findWithOptions(CompositeFindOptions.limit(Map.of(0, 0),2));
        assertThat(movieQueryCursor1.totalCount()).isEqualTo(3);
        assertThat(movieQueryCursor1.size()).isEqualTo(2);
        assertThat(movieQueryCursor1.toList()).hasSize(2);
        assertThat(movieQueryCursor1.hasMore()).isTrue();
        Map<Integer, Integer> nextOffsets = ((CompositeCursor) movieQueryCursor1).getNextOffsets();
        assertThat(nextOffsets).containsExactlyEntriesOf(Map.of(0, 2));
        Cursor movieQueryCursor2 = theStore.findWithOptions(CompositeFindOptions.limit(nextOffsets,2));
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

        Cursor movieQueryCursor1 = theStore.findWithOptions(Filters.gt("year", 2000), CompositeFindOptions.limit(Map.of(0, 0),1));
        assertThat(movieQueryCursor1.totalCount()).isEqualTo(2);
        assertThat(movieQueryCursor1.size()).isEqualTo(1);
        assertThat(movieQueryCursor1.toList()).hasSize(1);
        assertThat(movieQueryCursor1.hasMore()).isTrue();
        Map<Integer, Integer> nextOffsets = ((CompositeCursor) movieQueryCursor1).getNextOffsets();
        assertThat(nextOffsets).containsExactlyEntriesOf(Map.of(0, 1));
        Cursor movieQueryCursor2 = theStore.findWithOptions(Filters.gt("year", 2000), CompositeFindOptions.limit(nextOffsets,1));
        assertThat(movieQueryCursor2.totalCount()).isEqualTo(2);
        assertThat(movieQueryCursor2.size()).isEqualTo(1);
        assertThat(movieQueryCursor2.toList()).hasSize(1);
        assertThat(movieQueryCursor2.hasMore()).isFalse();
    }

    @Test
    public void shouldSupportFindAcrossMultipleStores() throws IOException {
        final WritableDocumentStore<String> store = newStoreInstance(3);
        stubProviderTwo.addStore(storeName, store);

        stubOneUnderlying.put(matrix1.code(), new Document(objectMapper.convertValue(matrix1, HashMap.class)));
        stubOneUnderlying.put(matrix2.code(), new Document(objectMapper.convertValue(matrix2, HashMap.class)));

        store.put(matrix3.code(), new Document(objectMapper.convertValue(matrix3, HashMap.class)));
        store.put(speed.code(), new Document(objectMapper.convertValue(speed, HashMap.class)));

        Cursor movieQueryCursor = theStore.findWithOptions(Filters.gt("year", 2000), CompositeFindOptions.sort("year", Ascending));
        assertThat(movieQueryCursor.toList()).map((d) -> d.get("code")).containsExactlyInAnyOrder(matrix2.code(), matrix3.code());
    }

    @Test
    public void shouldSupportLimitAcrossMultipleStores() throws IOException {
        final WritableDocumentStore<String> store = newStoreInstance(1);
        stubProviderTwo.addStore(storeName, store);

        stubOneUnderlying.put(matrix1.code(), new Document(objectMapper.convertValue(matrix1, HashMap.class)));
        stubOneUnderlying.put(matrix2.code(), new Document(objectMapper.convertValue(matrix2, HashMap.class)));

        store.put(matrix3.code(), new Document(objectMapper.convertValue(matrix3, HashMap.class)));
        store.put(matrix4.code(), new Document(objectMapper.convertValue(matrix4, HashMap.class)));
        store.put(speed.code(), new Document(objectMapper.convertValue(speed, HashMap.class)));

        Cursor movieQueryCursorPage1 = theStore.findWithOptions(CompositeFindOptions.sort("year", Ascending).thenLimit(Map.of(0, 0, 1, 0), 2));
        assertThat(movieQueryCursorPage1.toList()).map((d) -> d.get("code")).containsExactly(speed.code(), matrix1.code());
        assertThat(movieQueryCursorPage1.hasMore()).isTrue();
        Cursor movieQueryCursorPage2 = theStore.findWithOptions(CompositeFindOptions.sort("year", Ascending).thenLimit(((CompositeCursor) movieQueryCursorPage1).getNextOffsets(), 2));
        assertThat(movieQueryCursorPage2.toList()).map((d) -> d.get("code")).containsExactlyInAnyOrder(matrix2.code(), matrix3.code());
        assertThat(movieQueryCursorPage2.hasMore()).isTrue();
        Cursor movieQueryCursorPage3 = theStore.findWithOptions(CompositeFindOptions.sort("year", Ascending).thenLimit(((CompositeCursor) movieQueryCursorPage2).getNextOffsets(), 2));
        assertThat(movieQueryCursorPage3.toList()).map((d) -> d.get("code")).containsExactly(matrix4.code());
        assertThat(movieQueryCursorPage3.hasMore()).isFalse();
    }

    @Test
    public void shouldSupportLimitAcrossMultipleStoresWithAllPageSizes() throws IOException {
        final WritableDocumentStore<String> store = newStoreInstance(1);
        stubProviderTwo.addStore(storeName, store);

        stubOneUnderlying.put(matrix1.code(), new Document(objectMapper.convertValue(matrix1, HashMap.class)));
        stubOneUnderlying.put(matrix2.code(), new Document(objectMapper.convertValue(matrix2, HashMap.class)));

        store.put(matrix3.code(), new Document(objectMapper.convertValue(matrix3, HashMap.class)));
        store.put(matrix4.code(), new Document(objectMapper.convertValue(matrix4, HashMap.class)));
        store.put(speed.code(), new Document(objectMapper.convertValue(speed, HashMap.class)));

        Cursor movieQueryCursorPageSize1 = theStore.findWithOptions(CompositeFindOptions.sort("rating", Descending).thenLimit(1));
        assertThat(movieQueryCursorPageSize1.toList()).map((d) -> d.get("code")).containsExactly(matrix1.code());
        assertThat(movieQueryCursorPageSize1.hasMore()).isTrue();

        Cursor movieQueryCursorPageSize2 = theStore.findWithOptions(CompositeFindOptions.sort("rating", Descending).thenLimit(2));
        assertThat(movieQueryCursorPageSize2.toList()).map((d) -> d.get("code")).containsExactly(matrix1.code(), speed.code());
        assertThat(movieQueryCursorPageSize2.hasMore()).isTrue();

        Cursor movieQueryCursorPageSize3 = theStore.findWithOptions(CompositeFindOptions.sort("rating", Descending).thenLimit(3));
        assertThat(movieQueryCursorPageSize3.toList()).map((d) -> d.get("code")).containsExactly(matrix1.code(), speed.code(), matrix2.code());
        assertThat(movieQueryCursorPageSize3.hasMore()).isTrue();

        Cursor movieQueryCursorPageSize4 = theStore.findWithOptions(CompositeFindOptions.sort("rating", Descending).thenLimit(4));
        assertThat(movieQueryCursorPageSize4.toList()).map((d) -> d.get("code")).containsExactly(matrix1.code(), speed.code(), matrix2.code(), matrix3.code());
        assertThat(movieQueryCursorPageSize4.hasMore()).isTrue();

        Cursor movieQueryCursorPageSize5 = theStore.findWithOptions(CompositeFindOptions.sort("rating", Descending).thenLimit(5));
        assertThat(movieQueryCursorPageSize5.toList()).map((d) -> d.get("code")).containsExactly(matrix1.code(), speed.code(), matrix2.code(), matrix3.code(), matrix4.code());
        assertThat(movieQueryCursorPageSize5.hasMore()).isFalse();

        Cursor movieQueryCursorPage1 = theStore.findWithOptions(CompositeFindOptions.sort("rating", Descending).thenLimit(2));
        assertThat(movieQueryCursorPage1.toList()).map((d) -> d.get("code")).containsExactly(matrix1.code(), speed.code());
        assertThat(movieQueryCursorPage1.hasMore()).isTrue();

        Cursor movieQueryCursorPage2 = theStore.findWithOptions(CompositeFindOptions.sort("rating", Descending).thenLimit(((CompositeCursor) movieQueryCursorPage1).getNextOffsets(), 2));
        assertThat(movieQueryCursorPage2.toList()).map((d) -> d.get("code")).containsExactly(matrix2.code(), matrix3.code());
        assertThat(movieQueryCursorPage2.hasMore()).isTrue();

        Cursor movieQueryCursorPage3 = theStore.findWithOptions(CompositeFindOptions.sort("rating", Descending).thenLimit(((CompositeCursor) movieQueryCursorPage2).getNextOffsets(), 2));
        assertThat(movieQueryCursorPage3.toList()).map((d) -> d.get("code")).containsExactly(matrix4.code());
        assertThat(movieQueryCursorPage3.hasMore()).isFalse();
    }

    // TODO: we don't expect partitions to change, and if they do pagination should be reset either way, so should we be more strict on this?
    @Test
    public void shouldSupportOffsetsForUnknownPartitions() throws IOException {
        final WritableDocumentStore<String> store = newStoreInstance(1);
        stubProviderTwo.addStore(storeName, store);

        Cursor movieQueryCursorPage1 = theStore.findWithOptions(CompositeFindOptions.sort("rating", Descending).thenLimit(2));
        assertThat(movieQueryCursorPage1.toList()).isEmpty();
        assertThat(movieQueryCursorPage1.hasMore()).isFalse();

        Cursor movieQueryCursorPage2 = theStore.findWithOptions(CompositeFindOptions.sort("rating", Descending).thenLimit(Map.of(0, 0, 1, 0, 2, 1, 3, 5), 2));
        assertThat(movieQueryCursorPage2.toList()).isEmpty();
        assertThat(movieQueryCursorPage2.hasMore()).isFalse();
    }

    // TODO: what if the state store shrinks as we paginate through it, due to retention policies or tombstone records?
    @Test
    public void shouldThrowExceptionOnInvalidOffsetsForExistingPartitions() throws IOException {
        final WritableDocumentStore<String> store = newStoreInstance(1);
        stubProviderTwo.addStore(storeName, store);

        assertThrows(ValidationException.class, () -> theStore.findWithOptions(CompositeFindOptions.sort("rating", Descending).thenLimit(Map.of(0, 999, 1, 0), 2)));
    }

    @Test
    public void shouldSupportLimitAcrossMultipleStoresWithAllElementsOfFirstPageInSamePartition() throws IOException {
        final WritableDocumentStore<String> store = newStoreInstance(1);
        stubProviderTwo.addStore(storeName, store);

        stubOneUnderlying.put(matrix1.code(), new Document(objectMapper.convertValue(matrix1, HashMap.class)));
        stubOneUnderlying.put(speed.code(), new Document(objectMapper.convertValue(speed, HashMap.class)));

        store.put(matrix2.code(), new Document(objectMapper.convertValue(matrix2, HashMap.class)));
        store.put(matrix3.code(), new Document(objectMapper.convertValue(matrix3, HashMap.class)));
        store.put(matrix4.code(), new Document(objectMapper.convertValue(matrix4, HashMap.class)));

        Cursor movieQueryCursorPage1 = theStore.findWithOptions(CompositeFindOptions.sort("rating", Descending).thenLimit(2));
        assertThat(movieQueryCursorPage1.toList()).map((d) -> d.get("code")).containsExactly(matrix1.code(), speed.code());
        assertThat(movieQueryCursorPage1.hasMore()).isTrue();

        Cursor movieQueryCursorPage2 = theStore.findWithOptions(CompositeFindOptions.sort("rating", Descending).thenLimit(((CompositeCursor) movieQueryCursorPage1).getNextOffsets(), 2));
        assertThat(movieQueryCursorPage2.toList()).map((d) -> d.get("code")).containsExactly(matrix2.code(), matrix3.code());
        assertThat(movieQueryCursorPage2.hasMore()).isTrue();

        Cursor movieQueryCursorPage3 = theStore.findWithOptions(CompositeFindOptions.sort("rating", Descending).thenLimit(((CompositeCursor) movieQueryCursorPage2).getNextOffsets(), 2));
        assertThat(movieQueryCursorPage3.toList()).map((d) -> d.get("code")).containsExactly(matrix4.code());
        assertThat(movieQueryCursorPage3.hasMore()).isFalse();
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
    private File getNewStoreDir() throws IOException {
        File storeDir = new File("test-store-dir/" + storeName);
        Path pathToBeDeleted = storeDir.toPath();
        if (storeDir.exists()) {
            Files.walk(pathToBeDeleted)
                    .sorted(Comparator.reverseOrder())
                    .map(Path::toFile)
                    .forEach(File::delete);
        }
        return storeDir;
    }

}