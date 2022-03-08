package io.techasylum.kafka.statestore.document.no2;

import io.techasylum.kafka.statestore.document.QueryCursor;
import io.techasylum.kafka.statestore.document.ReadOnlyObjectDocumentStore;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.internals.StateStoreProvider;
import org.dizitart.no2.*;
import org.dizitart.no2.event.ChangeListener;
import org.dizitart.no2.exceptions.NotIdentifiableException;
import org.dizitart.no2.exceptions.ValidationException;
import org.dizitart.no2.mapper.NitriteMapper;
import org.dizitart.no2.meta.Attributes;
import org.dizitart.no2.objects.ObjectFilter;
import org.dizitart.no2.objects.ObjectRepository;

import java.lang.reflect.Field;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.dizitart.no2.Constants.DOC_ID;
import static org.dizitart.no2.IndexOptions.indexOptions;
import static org.dizitart.no2.UpdateOptions.updateOptions;
import static org.dizitart.no2.exceptions.ErrorCodes.*;
import static org.dizitart.no2.exceptions.ErrorCodes.VE_OBJ_REMOVE_NULL_OBJECT;
import static org.dizitart.no2.exceptions.ErrorMessage.*;
import static org.dizitart.no2.util.ObjectUtils.*;
import static org.dizitart.no2.util.ObjectUtils.getIdField;
import static org.dizitart.no2.util.ValidationUtils.notNull;

/**
 *
 *
 * @param <T> value type
 */
public class CompositeObjectRepository<T> implements ObjectRepository<T> {

    private NitriteCollection collection;
    private Class<T> type;
    private NitriteMapper nitriteMapper;
    private Field idField;

    /**
     *
     * OLD
     *
     */

    private final StateStoreProvider storeProvider;
//    private final QueryableStoreType<ReadOnlyObjectDocumentStore<K, V, F, O>> storeType;
    private final String storeName;

    public CompositeObjectRepository(final StateStoreProvider storeProvider,
//                                     final QueryableStoreType<ReadOnlyObjectDocumentStore<K, V, F, O>> storeType,
                                     final String storeName) {
        this.storeProvider = storeProvider;
//        this.storeType = storeType;
        this.storeName = storeName;
    }

    /**
     *
     * NEW
     *
     */

    @Override
    public void createIndex(String field, IndexOptions indexOptions) {
        validateCollection();
        notNull(field, errorMessage("field can not be null", VE_OBJ_CREATE_INDEX_NULL_FIELD));
        collection.createIndex(field, indexOptions);
    }

    @Override
    public void rebuildIndex(String field, boolean async) {
        validateCollection();
        collection.rebuildIndex(field, async);
    }

    @Override
    public Collection<Index> listIndices() {
        validateCollection();
        return collection.listIndices();
    }

    @Override
    public boolean hasIndex(String field) {
        validateCollection();
        return collection.hasIndex(field);
    }

    @Override
    public boolean isIndexing(String field) {
        validateCollection();
        return collection.isIndexing(field);
    }

    @Override
    public void dropIndex(String field) {
        validateCollection();
        collection.dropIndex(field);
    }

    @Override
    public void dropAllIndices() {
        validateCollection();
        collection.dropAllIndices();
    }

    @SafeVarargs
    @Override
    public final WriteResult insert(T object, T... others) {
        validateCollection();
        return collection.insert(asDocument(object, false), asDocuments(others, false));
    }

    @Override
    public WriteResult insert(T[] objects) {
        validateCollection();
        return collection.insert(asDocuments(objects, false));
    }

    @Override
    public WriteResult update(T element) {
        return update(element, false);
    }

    @Override
    public WriteResult update(T element, boolean upsert) {
        if (idField == null) {
            throw new NotIdentifiableException(OBJ_UPDATE_FAILED_AS_NO_ID_FOUND);
        }
        return update(createUniqueFilter(element, idField), element, upsert);
    }

    @Override
    public WriteResult update(ObjectFilter filter, T update) {
        return update(filter, update, false);
    }

    @Override
    public WriteResult update(ObjectFilter filter, T update, boolean upsert) {
        validateCollection();
        notNull(update, errorMessage("update can not be null", VE_OBJ_UPDATE_NULL_OBJECT));

        Document updateDocument = asDocument(update, true);
        removeIdFields(updateDocument);
        return collection.update(prepare(filter), updateDocument, updateOptions(upsert, true));
    }

    @Override
    public WriteResult update(ObjectFilter filter, Document update) {
        return update(filter, update, false);
    }

    @Override
    public WriteResult update(ObjectFilter filter, Document update, boolean justOnce) {
        validateCollection();
        notNull(update, errorMessage("update can not be null", VE_OBJ_UPDATE_NULL_DOCUMENT));

        removeIdFields(update);
        serializeFields(update);
        return collection.update(prepare(filter), update, updateOptions(false, justOnce));
    }

    @Override
    public WriteResult remove(T element) {
        notNull(element, errorMessage("element can not be null", VE_OBJ_REMOVE_NULL_OBJECT));
        if (idField == null) {
            throw new NotIdentifiableException(OBJ_REMOVE_FAILED_AS_NO_ID_FOUND);
        }
        return remove(createUniqueFilter(element, idField));
    }

    @Override
    public WriteResult remove(ObjectFilter filter) {
        validateCollection();
        return remove(prepare(filter), new RemoveOptions());
    }

    @Override
    public WriteResult remove(ObjectFilter filter, RemoveOptions removeOptions) {
        validateCollection();
        return collection.remove(prepare(filter), removeOptions);
    }

    @Override
    public org.dizitart.no2.objects.Cursor<T> find() {
        validateCollection();
        return /*new ObjectCursor<>(nitriteMapper, collection.find(), type)*/null;
    }

    @Override
    public org.dizitart.no2.objects.Cursor<T> find(ObjectFilter filter) {
        validateCollection();
        return /*new ObjectCursor<>(nitriteMapper,
                collection.find(prepare(filter)), type)*/null;
    }

    @Override
    public org.dizitart.no2.objects.Cursor<T> find(FindOptions findOptions) {
        validateCollection();
        return /*new ObjectCursor<>(nitriteMapper,
                collection.find(findOptions), type)*/null;
    }

    @Override
    public org.dizitart.no2.objects.Cursor<T> find(ObjectFilter filter, FindOptions findOptions) {
        validateCollection();
        return /*new ObjectCursor<>(nitriteMapper,
                collection.find(prepare(filter), findOptions), type)*/null;
    }

    @Override
    public T getById(NitriteId nitriteId) {
        validateCollection();
        Document byId = collection.getById(nitriteId);
        if (byId == null) return null;
        Document document = new Document(byId);
        document.remove(DOC_ID);
        return nitriteMapper.asObject(document, type);
    }

    @Override
    public void drop() {
        validateCollection();
        collection.drop();
    }

    @Override
    public boolean isDropped() {
        validateCollection();
        return collection.isDropped();
    }

    @Override
    public String getName() {
        return collection.getName();
    }

    @Override
    public long size() {
        validateCollection();
        return collection.size();
    }

    @Override
    public boolean isClosed() {
        validateCollection();
        return collection.isClosed();
    }

    @Override
    public void close() {
        validateCollection();
        collection.close();
    }

    @Override
    public Class<T> getType() {
        return type;
    }

    @Override
    public NitriteCollection getDocumentCollection() {
        return collection;
    }

    @Override
    public Attributes getAttributes() {
        return collection.getAttributes();
    }

    @Override
    public void setAttributes(Attributes attributes) {
        collection.setAttributes(attributes);
    }

    @Override
    public void register(ChangeListener listener) {
        collection.register(listener);
    }

    @Override
    public void deregister(ChangeListener listener) {
        collection.deregister(listener);
    }

    private void validateCollection() {
        if (collection == null) {
            throw new ValidationException(REPOSITORY_NOT_INITIALIZED);
        }
    }

    private Document asDocument(T object, boolean update) {
        return toDocument(object, nitriteMapper, idField, update);
    }

    private Document[] asDocuments(T[] others, boolean update) {
        if (others == null || others.length == 0) return null;
        Document[] documents = new Document[others.length];
        for (int i = 0; i < others.length; i++) {
            documents[i] = asDocument(others[i], update);
        }
        return documents;
    }

    private void initRepository(NitriteContext nitriteContext) {
        nitriteMapper = nitriteContext.getNitriteMapper();
        createIndexes();
    }

    private void createIndexes() {
        validateCollection();
        Set<org.dizitart.no2.objects.Index> indexes = extractIndices(nitriteMapper, type);
        for (org.dizitart.no2.objects.Index idx : indexes) {
            if (!collection.hasIndex(idx.value())) {
                collection.createIndex(idx.value(), indexOptions(idx.type(), false));
            }
        }

        idField = getIdField(nitriteMapper, type);
        if (idField != null) {
            if (!collection.hasIndex(idField.getName())) {
                collection.createIndex(idField.getName(), indexOptions(IndexType.Unique));
            }
        }
    }

    private ObjectFilter prepare(ObjectFilter objectFilter) {
        if (objectFilter != null) {
            objectFilter.setNitriteMapper(nitriteMapper);
            return objectFilter;
        }
        return null;
    }

    private void removeIdFields(Document document) {
        document.remove(DOC_ID);
        if (idField != null && idField.getType() == NitriteId.class) {
            document.remove(idField.getName());
        }
    }

    private void serializeFields(Document document) {
        if (document != null) {
            for (KeyValuePair keyValuePair : document) {
                String key = keyValuePair.getKey();
                Object value = keyValuePair.getValue();
                Object serializedValue;
                if (nitriteMapper.isValueType(value)) {
                    serializedValue = nitriteMapper.asValue(value);
                } else {
                    serializedValue = nitriteMapper.asDocument(value);
                }
                document.put(key, serializedValue);
            }
        }
    }

    /**
     *
     *
     *
     *
     *
     *
     *
     *
     *
     *
     *
     *
     */


/**
    @Override
    public V get(final K key) {
        Objects.requireNonNull(key);
        final List<ReadOnlyObjectDocumentStore<K, V, F, O>> stores = storeProvider.stores(storeName, storeType);
        // TODO: use the KeyMetadata to resolve the partition directly and optimize the lookup instead of going through each of the partitions
        for (final ReadOnlyObjectDocumentStore<K, V, F, O> store : stores) {
            try {
                final V result = store.get(key);
                if (result != null) {
                    return result;
                }
            } catch (final InvalidStateStoreException e) {
                throw new InvalidStateStoreException("State store is not available anymore and may have been migrated to another instance; please re-discover its location from the state metadata.");
            }

        }
        return null;
    }

    @Override
    public QueryCursor<V> find(F filter) {
        Objects.requireNonNull(filter);
        final List<ReadOnlyObjectDocumentStore<K, V, F, O>> stores = storeProvider.stores(storeName, storeType);
        for (final ReadOnlyObjectDocumentStore<K, V, F, O> store : stores) {
            try {
                final QueryCursor<V> result = store.find(filter);
                // TODO: merge queryCursors of all partitions
                if (result != null) {
                    return result;
                }
            } catch (final InvalidStateStoreException e) {
                throw new InvalidStateStoreException("State store is not available anymore and may have been migrated to another instance; please re-discover its location from the state metadata.");
            }

        }
        return null;
    }

    @Override
    public QueryCursor<V> findWithOptions(O options) {
        Objects.requireNonNull(options);
        final List<ReadOnlyObjectDocumentStore<K, V, F, O>> stores = storeProvider.stores(storeName, storeType);
        for (final ReadOnlyObjectDocumentStore<K, V, F, O> store : stores) {
            try {
                final QueryCursor<V> result = store.findWithOptions(options);
                // TODO: merge queryCursors of all partitions
                if (result != null) {
                    return result;
                }
            } catch (final InvalidStateStoreException e) {
                throw new InvalidStateStoreException("State store is not available anymore and may have been migrated to another instance; please re-discover its location from the state metadata.");
            }

        }
        return null;
    }

    @Override
    public QueryCursor<V> findWithOptions(F filter, O options) {
        Objects.requireNonNull(filter);
        Objects.requireNonNull(options);
        final List<ReadOnlyObjectDocumentStore<K, V, F, O>> stores = storeProvider.stores(storeName, storeType);
        for (final ReadOnlyObjectDocumentStore<K, V, F, O> store : stores) {
            try {
                final QueryCursor<V> result = store.findWithOptions(filter, options);
                // TODO: merge queryCursors of all partitions
                if (result != null) {
                    return result;
                }
            } catch (final InvalidStateStoreException e) {
                throw new InvalidStateStoreException("State store is not available anymore and may have been migrated to another instance; please re-discover its location from the state metadata.");
            }

        }
        return null;
    }
*/
}
