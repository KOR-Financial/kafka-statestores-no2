package io.techasylum.kafka.statestore.document;

import io.techasylum.kafka.statestore.document.internals.InternalMockProcessorContext;
import io.techasylum.kafka.statestore.document.internals.MockRecordCollector;
import io.techasylum.kafka.statestore.document.internals.StateStoreProviderStub;
import io.techasylum.kafka.statestore.document.internals.WrappingStoreProvider;
import io.techasylum.kafka.statestore.document.no2.movies.Movie;
import io.techasylum.kafka.statestore.document.no2.movies.SetMovieCommand;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StoreQueryParameters;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.StateSerdes;
import org.apache.kafka.streams.state.internals.CompositeReadOnlyKeyValueStore;
import org.assertj.core.api.Assertions;
import org.dizitart.no2.FindOptions;
import org.dizitart.no2.objects.ObjectFilter;
import org.dizitart.no2.objects.filters.ObjectFilters;
import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonSerde;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.NoSuchElementException;

import static io.techasylum.kafka.statestore.document.internals.StreamsTestUtils.toList;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.*;

public class CompositeReadOnlyDocumentStoreTest {

    private final String storeName = "my-store";
    private StateStoreProviderStub stubProviderTwo;
    private DocumentStore<String, Movie, ObjectFilter, FindOptions> stubOneUnderlying;
    private DocumentStore<String, Movie, ObjectFilter, FindOptions> otherUnderlyingStore;
    private CompositeReadOnlyDocumentStore<String, Movie, ObjectFilter, FindOptions> theStore;
    private final Serde<Movie> movieSerde = new JsonSerde<>(Movie.class);

    private final Movie matrix1 = new Movie("MTRX1", "The Matrix", 1999, 8.7f);
    private final Movie matrix2 = new Movie("MTRX2", "The Matrix Reloaded", 2003, 7.2f);
    private final Movie matrix3 = new Movie("MTRX3", "The Matrix Revolutions", 2003, 6.8f);
    private final Movie matrix4 = new Movie("MTRX4", "The Matrix Resurrections", 2021, 6.0f);
    private final Movie speed = new Movie("SPEED", "Speed", 1994, 7.2f);

    @BeforeEach
    public void before() throws IOException {
        final StateStoreProviderStub stubProviderOne = new StateStoreProviderStub(false);
        stubProviderTwo = new StateStoreProviderStub(false);

        stubOneUnderlying = newStoreInstance();
        stubProviderOne.addStore(storeName, stubOneUnderlying);
        otherUnderlyingStore = newStoreInstance();
        stubProviderOne.addStore("other-store", otherUnderlyingStore);
        theStore = new CompositeReadOnlyDocumentStore<>(
                new WrappingStoreProvider(asList(stubProviderOne, stubProviderTwo), StoreQueryParameters.fromNameAndType(storeName, QueryableDocumentStoreTypes.documentStore())),
                QueryableDocumentStoreTypes.documentStore(),
                storeName
        );
    }

    private DocumentStore<String, Movie, ObjectFilter, FindOptions> newStoreInstance() throws IOException {
        final DocumentStore<String, Movie, ObjectFilter, FindOptions> store = DocumentStores.nitriteStore(storeName, Serdes.String(), movieSerde, Movie.class, "code").build();
        File storeDir = getNewStoreDir();

        @SuppressWarnings("rawtypes") final InternalMockProcessorContext context =
                new InternalMockProcessorContext<>(
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
        stubOneUnderlying.put(speed.code(), speed);
        assertEquals(speed, theStore.get(speed.code()));
    }

    @Test
    public void shouldNotGetValuesFromOtherStores() {
        otherUnderlyingStore.put(matrix1.code(), matrix1);
        assertNull(theStore.get(matrix1.code()));
    }

    @Test
    public void shouldFindValueWithFieldQuery() {
        stubOneUnderlying.put(matrix2.code(), matrix2);
        final QueryCursor<Movie> queryCursor = theStore.find(ObjectFilters.eq("title", matrix2.title()));
        assertThat(queryCursor.firstOrDefault()).isEqualTo(matrix2);
    }

    @Test
    public void shouldFindValueForKeyWhenMultiStores() throws IOException {
        final DocumentStore<String, Movie, ObjectFilter, FindOptions> cache = newStoreInstance();
        stubProviderTwo.addStore(storeName, cache);

        cache.put(matrix1.code(), matrix1);
        stubOneUnderlying.put(matrix2.code(), matrix2);

        assertEquals(matrix1, theStore.get(matrix1.code()));
        assertEquals(matrix2, theStore.get(matrix2.code()));
    }

    @Test
    public void shouldFindMultipleValues() {
        stubOneUnderlying.put(matrix1.code(), matrix1);
        stubOneUnderlying.put(matrix2.code(), matrix2);
        stubOneUnderlying.put(matrix3.code(), matrix3);

        QueryCursor<Movie> movieQueryCursor = theStore.find(ObjectFilters.gt("year", 2000));
        assertThat(movieQueryCursor.totalCount()).isEqualTo(2);
        assertThat(movieQueryCursor.size()).isEqualTo(2);
        assertThat(movieQueryCursor.firstOrDefault()).isEqualTo(matrix2);
        assertThat(movieQueryCursor.toList()).hasSize(2);
        assertThat(movieQueryCursor.toList()).containsExactly(matrix2, matrix3);
    }

    @Test
    public void shouldSupportPagination() {
        stubOneUnderlying.put(matrix1.code(), matrix1);
        stubOneUnderlying.put(matrix2.code(), matrix2);
        stubOneUnderlying.put(matrix3.code(), matrix3);

        QueryCursor<Movie> movieQueryCursor1 = theStore.findWithOptions(FindOptions.limit(0,2));
        assertThat(movieQueryCursor1.totalCount()).isEqualTo(3);
        assertThat(movieQueryCursor1.size()).isEqualTo(2);
        assertThat(movieQueryCursor1.firstOrDefault()).isEqualTo(matrix1);
        assertThat(movieQueryCursor1.toList()).hasSize(2);
        assertThat(movieQueryCursor1.toList()).containsExactly(matrix1, matrix2);
        assertThat(movieQueryCursor1.hasMore()).isTrue();
        QueryCursor<Movie> movieQueryCursor2 = theStore.findWithOptions(FindOptions.limit(2,2));
        assertThat(movieQueryCursor2.totalCount()).isEqualTo(3);
        assertThat(movieQueryCursor2.size()).isEqualTo(1);
        assertThat(movieQueryCursor2.firstOrDefault()).isEqualTo(matrix3);
        assertThat(movieQueryCursor2.toList()).hasSize(1);
        assertThat(movieQueryCursor2.toList()).containsExactly(matrix3);
        assertThat(movieQueryCursor2.hasMore()).isFalse();
    }

    @Test
    public void shouldSupportPaginationWhileFiltering() {
        stubOneUnderlying.put(matrix1.code(), matrix1);
        stubOneUnderlying.put(matrix2.code(), matrix2);
        stubOneUnderlying.put(matrix3.code(), matrix3);

        QueryCursor<Movie> movieQueryCursor1 = theStore.findWithOptions(ObjectFilters.gt("year", 2000), FindOptions.limit(0,1));
        assertThat(movieQueryCursor1.totalCount()).isEqualTo(2);
        assertThat(movieQueryCursor1.size()).isEqualTo(1);
        assertThat(movieQueryCursor1.firstOrDefault()).isEqualTo(matrix2);
        assertThat(movieQueryCursor1.toList()).hasSize(1);
        assertThat(movieQueryCursor1.toList()).containsExactly(matrix2);
        assertThat(movieQueryCursor1.hasMore()).isTrue();
        QueryCursor<Movie> movieQueryCursor2 = theStore.findWithOptions(ObjectFilters.gt("year", 2000), FindOptions.limit(1,1));
        assertThat(movieQueryCursor2.totalCount()).isEqualTo(2);
        assertThat(movieQueryCursor2.size()).isEqualTo(1);
        assertThat(movieQueryCursor2.firstOrDefault()).isEqualTo(matrix3);
        assertThat(movieQueryCursor2.toList()).hasSize(1);
        assertThat(movieQueryCursor2.toList()).containsExactly(matrix3);
        assertThat(movieQueryCursor2.hasMore()).isFalse();
    }


    @Test
    public void shouldSupportFindAcrossMultipleStores() throws IOException {
        final DocumentStore<String, Movie, ObjectFilter, FindOptions> cache = newStoreInstance();
        stubProviderTwo.addStore(storeName, cache);

        stubOneUnderlying.put(matrix1.code(), matrix1);
        stubOneUnderlying.put(matrix2.code(), matrix2);

        cache.put(matrix3.code(), matrix3);
        cache.put(speed.code(), speed);

        QueryCursor<Movie> movieQueryCursor = theStore.find(ObjectFilters.gt("year", 2000));
        assertThat(movieQueryCursor.toList()).containsExactly(matrix2, matrix3);
    }

    /**
    @Test
    public void shouldSupportPrefixScanAcrossMultipleKVStores() {
        final DocumentStore<String, Movie, ObjectFilter, FindOptions> cache = newStoreInstance();
        stubProviderTwo.addStore(storeName, cache);

        stubOneUnderlying.put("a", "a");
        stubOneUnderlying.put("b", "b");
        stubOneUnderlying.put("z", "z");

        cache.put("aa", "c");
        cache.put("ab", "d");
        cache.put("x", "x");

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
        final DocumentStore<String, Movie, ObjectFilter, FindOptions> cache = newStoreInstance();
        stubProviderTwo.addStore(storeName, cache);

        stubOneUnderlying.put("a", "a");
        stubOneUnderlying.put("b", "b");
        stubOneUnderlying.put("z", "z");

        cache.put("c", "c");
        cache.put("d", "d");
        cache.put("x", "x");

        final List<KeyValue<String, String>> results = toList(theStore.reverseRange("a", "e"));
        assertTrue(results.contains(new KeyValue<>("a", "a")));
        assertTrue(results.contains(new KeyValue<>("b", "b")));
        assertTrue(results.contains(new KeyValue<>("c", "c")));
        assertTrue(results.contains(new KeyValue<>("d", "d")));
        assertEquals(4, results.size());
    }

    @Test
    public void shouldSupportAllAcrossMultipleStores() {
        final DocumentStore<String, Movie, ObjectFilter, FindOptions> cache = newStoreInstance();
        stubProviderTwo.addStore(storeName, cache);

        stubOneUnderlying.put("a", "a");
        stubOneUnderlying.put("b", "b");
        stubOneUnderlying.put("z", "z");

        cache.put("c", "c");
        cache.put("d", "d");
        cache.put("x", "x");

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
        final DocumentStore<String, Movie, ObjectFilter, FindOptions> cache = newStoreInstance();
        stubProviderTwo.addStore(storeName, cache);

        stubOneUnderlying.put("a", "a");
        stubOneUnderlying.put("b", "b");
        stubOneUnderlying.put("z", "z");

        cache.put("c", "c");
        cache.put("d", "d");
        cache.put("x", "x");

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
        final DocumentStore<String, Movie, ObjectFilter, FindOptions> cache = newStoreInstance();
        stubProviderTwo.addStore(storeName, cache);

        stubOneUnderlying.put("a", "a");
        stubOneUnderlying.put("b", "b");
        stubOneUnderlying.put("z", "z");

        cache.put("c", "c");
        cache.put("d", "d");
        cache.put("x", "x");

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