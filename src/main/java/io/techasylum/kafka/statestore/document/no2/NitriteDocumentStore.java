package io.techasylum.kafka.statestore.document.no2;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Function;

import io.techasylum.kafka.statestore.document.WritableDocumentStore;
import io.techasylum.kafka.statestore.document.serialization.DocumentSerde;
import org.apache.commons.lang3.NotImplementedException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.errors.ProcessorStateException;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.processor.StateStoreContext;
import org.apache.kafka.streams.processor.internals.InternalProcessorContext;
import org.apache.kafka.streams.processor.internals.ProcessorContextUtils;
import org.apache.kafka.streams.processor.internals.ProcessorStateManager;
import org.apache.kafka.streams.processor.internals.SerdeGetter;
import org.apache.kafka.streams.state.StateSerdes;
import org.dizitart.no2.Document;
import org.dizitart.no2.Filter;
import org.dizitart.no2.FindOptions;
import org.dizitart.no2.Index;
import org.dizitart.no2.IndexOptions;
import org.dizitart.no2.Nitrite;
import org.dizitart.no2.NitriteBuilder;
import org.dizitart.no2.NitriteCollection;
import org.dizitart.no2.exceptions.NitriteException;
import org.dizitart.no2.filters.Filters;
import org.dizitart.no2.objects.Cursor;
import org.slf4j.Logger;

import static org.apache.kafka.streams.kstream.internals.WrappingNullableUtils.prepareKeySerde;
import static org.apache.kafka.streams.kstream.internals.WrappingNullableUtils.prepareValueSerde;
import static org.apache.kafka.streams.processor.internals.ProcessorContextUtils.asInternalProcessorContext;
import static org.dizitart.no2.UpdateOptions.updateOptions;
import static org.slf4j.LoggerFactory.getLogger;

public class NitriteDocumentStore<Key, Doc extends Document> implements WritableDocumentStore<Key, Doc> {

    private static final Logger logger = getLogger(NitriteDocumentStore.class);

    private int partition;
    private final String name;
    private final Serde<Key> keySerde;
    private final DocumentSerde<Doc> valueSerde;
    private final String keyFieldName;
    private final Function<Document, Doc> documentConverter;
    private final Map<String, IndexOptions> indices;
    private final List<NitriteCustomizer> customizers;

    private boolean enableLogging;

    private Nitrite db;
    private NitriteCollection collection;
    private StateSerdes<Key, Doc> serdes;

    InternalProcessorContext context;

    public NitriteDocumentStore(String name, Serde<Key> keySerde, DocumentSerde<Doc> valueSerde, String keyFieldName, Function<Document, Doc> documentConverter, Map<String, IndexOptions> indices, List<NitriteCustomizer> customizers, boolean enableLogging) {
        this.name = name;
        this.keySerde = keySerde;
        this.valueSerde = valueSerde;
        this.keyFieldName = keyFieldName;
        this.documentConverter = documentConverter;
        this.indices = indices;
        this.customizers = customizers;
        this.enableLogging = enableLogging;
    }

// == Store Properties ================================================================================================

    @Override
    public String name() {
        return name;
    }

    @Override
    public boolean persistent() {
        return false;
    }

    @Override
    public boolean isOpen() {
        return !(this.db == null || this.db.isClosed());
    }

// == Store Level Administration ======================================================================================

    @Override
    public void init(ProcessorContext context, StateStore root) {
        throw new NotImplementedException("deprecated.");
    }

    @Override
    public void init(StateStoreContext context, StateStore root) {
        this.context = asInternalProcessorContext(context);
        partition = context.taskId().partition();

        initStoreSerde(context);

        openDB(context.stateDir());

        List<String> existingIndices = collection.listIndices().stream().map(Index::getField).toList();
        Set<String> definedIndices = new HashSet<>(indices.keySet());

        ArrayList<String> existingIndicesClone = new ArrayList<>(existingIndices);
        existingIndicesClone.removeAll(definedIndices);
        logger.warn("Indices exist that have not been defined in the code: {}", existingIndicesClone);
        existingIndices.forEach(definedIndices::remove);

        this.indices.entrySet().stream()
                .filter((entry) -> definedIndices.contains(entry.getKey()))
                .forEach((entry) -> createIndex(entry.getKey(), entry.getValue()));

        context.register(root, new NitriteRestoreCallback<>(this));
    }

    private void initStoreSerde(final StateStoreContext context) {
        final String storeName = name();
        final String changelogTopic = ProcessorContextUtils.changelogFor(context, storeName);
        serdes = new StateSerdes<>(
                changelogTopic != null ?
                        changelogTopic :
                        ProcessorStateManager.storeChangelogTopic(context.applicationId(), storeName),
                prepareKeySerde(keySerde, new SerdeGetter(context)),
                prepareValueSerde(valueSerde, new SerdeGetter(context))
        );
    }

    @Override
    public void flush() {
        if (!isOpen()) return;
        this.db.commit();
    }

    @Override
    public void close() {
        if (!isOpen()) return;
        this.db.close();
        this.db = null;
    }

    private void validateStoreOpen() {
        if (!isOpen()) {
            throw new InvalidStateStoreException("Store " + name + " is currently closed");
        }
    }

    void openDB(final File stateDir) {
        File dbDir = new File(stateDir, name);

        try {
            Files.createDirectories(dbDir.getParentFile().toPath());
        } catch (final IOException fatal) {
            throw new ProcessorStateException(fatal);
        }

        NitriteBuilder builder = Nitrite.builder();
        for (NitriteCustomizer customizer : customizers) {
            builder = customizer.customize(builder);
        }

        builder = builder.filePath(dbDir);

        try {
            this.db = builder.openOrCreate();
            this.collection = this.db.getCollection(name);
        } catch (NitriteException ne) {
            throw new ProcessorStateException("Error opening store " + name + " at location " + dbDir, ne);
        }
    }

// == Operations ======================================================================================================

    @Override
    public Doc get(Key key) {
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();

        Cursor<Doc> items = convertCursor(this.collection.find(Filters.eq(keyFieldName, key)));

        int cnt = items.totalCount();
        if (cnt == 0) {
            return null;
        } else if (cnt == 1) {
            return items.firstOrDefault();
        } else {
            throw new ProcessorStateException(String.format("Multiple results for key %s!", key));
        }
    }

    @Override
    public Cursor<Doc> find(Filter filter) {
        Objects.requireNonNull(filter, "filter cannot be null");
        validateStoreOpen();

        return convertCursor(this.collection.find(filter));
    }

    @Override
    public Cursor<Doc> findWithOptions(FindOptions findOptions) {
        Objects.requireNonNull(findOptions, "findOptions cannot be null");
        validateStoreOpen();

        return convertCursor(this.collection.find(findOptions));
    }

    @Override
    public Cursor<Doc> findWithOptions(Filter filter, FindOptions findOptions) {
        Objects.requireNonNull(findOptions, "findOptions cannot be null");
        validateStoreOpen();

        return convertCursor(this.collection.find(filter, findOptions));
    }

    @Override
    public void put(Key key, Doc value) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(value, "value cannot be null");
        validateStoreOpen();

        this.store(key, value);
        if (enableLogging) {
            this.log(key, value);
        }
    }

    @Override
    public Doc putIfAbsent(Key key, Doc value) {
        Objects.requireNonNull(key, "key cannot be null");
        Objects.requireNonNull(value, "value cannot be null");
        validateStoreOpen();

        final Doc previous = this.storeIfAbsent(key, value);
        if (enableLogging && previous == null) {
            // then it was absent
            log(key, value);
        }
        return previous;
    }

    @Override
    public void putAll(List<KeyValue<Key, Doc>> entries) {
        Objects.requireNonNull(entries, "entries cannot be null");
        validateStoreOpen();

        this.storeAll(entries);
        if (enableLogging) {
            for (KeyValue<Key, Doc> entry : entries) {
                this.log(entry.key, entry.value);
            }
        }
    }

    @Override
    public Doc delete(Key key) {
        Objects.requireNonNull(key, "key cannot be null");
        validateStoreOpen();

        final Doc oldValue = this.remove(key);
        if (enableLogging) {
            log(key, null);
        }
        return oldValue;
    }

    void log(final Key key,
             final Doc value) {
        context.logChange(
                name(),
                Bytes.wrap(this.serdes.rawKey(key)),
                this.serdes.rawValue(value),
                context.timestamp());
    }

// == Internal Operations (no logging) ================================================================================

    protected synchronized void store(Key key, Doc value) {
        this.collection.update(Filters.eq(keyFieldName, key), value, updateOptions(true));
    }

    protected synchronized Doc storeIfAbsent(Key key, Doc value) {
        Doc existing = this.get(key);
        if (existing == null) {
            this.store(key, value);
        }

        return existing;
    }

    protected synchronized void storeAll(List<KeyValue<Key, Doc>> entries) {
        for (KeyValue<Key, Doc> entry : entries) {
            this.collection.update(Filters.eq(keyFieldName, entry.key), entry.value, updateOptions(true));
        }
    }

    protected Doc remove(Key key) {
        Doc result = this.get(key);
        if (result == null) {
            return null;
        }

        this.collection.remove(Filters.eq(keyFieldName, key));

        return result;
    }

    @Override
    public int getPartition() {
        return partition;
    }

    private Cursor<Doc> convertCursor(org.dizitart.no2.Cursor collectionCursor) {
        return new DocumentCursor<>(collectionCursor, documentConverter);
    }

// == Indexes =========================================================================================================

    @Override
    public void createIndex(String field, IndexOptions indexOptions) {
        collection.createIndex(field, indexOptions);
    }

    @Override
    public void rebuildIndex(String field, boolean async) {
        collection.rebuildIndex(field, async);
    }

    @Override
    public void dropIndex(String field) {
        collection.dropIndex(field);
    }

    @Override
    public void dropAllIndices() {
        collection.dropAllIndices();
    }

    @Override
    public boolean hasIndex(String field) {
        return collection.hasIndex(field);
    }

    @Override
    public boolean isIndexing(String field) {
        return collection.isIndexing(field);
    }

    @Override
    public Collection<Index> listIndices() {
        return collection.listIndices();
    }

// == Replay ==========================================================================================================

    private static class NitriteRestoreCallback<K, D extends Document> implements StateRestoreCallback {

        private final NitriteDocumentStore<K, D> store;

        private NitriteRestoreCallback(NitriteDocumentStore<K, D> store) {
            this.store = store;
        }

        // TODO: write IT to verify its workings !!!
        @Override
        public void restore(byte[] key, byte[] value) {
            K k = store.serdes.keyFrom(key);
            D v = store.serdes.valueFrom(value);
            if (v == null) {
                store.delete(k);
            } else {
                store.store(k, v);
            }
        }
    }
}
