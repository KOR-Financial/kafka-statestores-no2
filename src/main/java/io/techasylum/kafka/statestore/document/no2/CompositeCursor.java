package io.techasylum.kafka.statestore.document.no2;

import io.techasylum.kafka.statestore.document.QueryCursor;
import org.dizitart.no2.*;
import org.dizitart.no2.Document;
import org.dizitart.no2.exceptions.InvalidOperationException;
import org.dizitart.no2.internals.DocumentCursorInternals;
import org.dizitart.no2.objects.ObjectCursorInternals;
import org.dizitart.no2.store.NitriteMap;
import org.jetbrains.annotations.NotNull;

import java.text.Collator;
import java.util.*;
import java.util.function.Consumer;

import static org.dizitart.no2.exceptions.ErrorMessage.UNABLE_TO_SORT_ON_ARRAY;
import static org.dizitart.no2.util.DocumentUtils.getFieldValue;

public class CompositeCursor<V> implements QueryCursor<V> {

    private final Map<Integer, NitriteQueryCursorWrapper<V>> cursors;
    private final Map<Integer, DocumentCursorInternals> cursorsWithInternals = new HashMap<>();
    private final NitriteQueryCursorWrapper<V> compositeCursor;
    private final Set<NitriteId> compositeResultSet = new HashSet<>();
    private NitriteMap<NitriteId, Document> compositeUnderlyingMap = null;

    private final FindOptions findOptions;

    private List<Integer> offsetsPerPartition;

    private List<List<V>> compositeList;
    private boolean compositeHasMore;
    private int compositeSize;
    private int compositeTotalCount;

    public CompositeCursor(Map<Integer, NitriteQueryCursorWrapper<V>> cursors, FindOptions findOptions) {
        this.cursors = cursors;
        this.findOptions = findOptions;
//        calculateComposite();
        for (Map.Entry<Integer, NitriteQueryCursorWrapper<V>> cursorEntry : cursors.entrySet()) {
            ObjectCursorInternals objectCursorInternals = new ObjectCursorInternals(cursorEntry.getValue());
            DocumentCursorInternals documentCursorInternals = new DocumentCursorInternals(objectCursorInternals.getInternalCursor());
            compositeResultSet.addAll(documentCursorInternals.getResultSet());
            if (compositeUnderlyingMap == null) {
                compositeUnderlyingMap = documentCursorInternals.getUnderlyingMap();
            } else {
                for (Map.Entry<NitriteId, Document> entry : documentCursorInternals.getUnderlyingMap().entrySet()) {
                    compositeUnderlyingMap.putIfAbsent(entry.getKey(), entry.getValue());
                }
            }
            cursorsWithInternals.put(cursorEntry.getKey(), documentCursorInternals);
        }
        compositeCursor = new NitriteQueryCursorWrapper<V>(
                ObjectCursorInternals.createCursor(DocumentCursorInternals.createCursor(compositeResultSet, compositeUnderlyingMap)));
    }

//    private void calculateComposite() {
//        compositeList = cursors.values().stream().map(QueryCursor::toList).sorted(sort()).toList();
//        compositeHasMore = cursors.values().stream().map(QueryCursor::hasMore).reduce(Boolean::logicalOr).orElse(false);
//        compositeSize = cursors.values().stream().mapToInt(QueryCursor::size).sum();
//        compositeTotalCount = cursors.values().stream().mapToInt(QueryCursor::totalCount).sum();
//    }

    @NotNull
    @Override
    public Iterator<V> iterator() {
        return this.compositeCursor.iterator();
    }

    @Override
    public void forEach(Consumer<? super V> action) {
        this.compositeCursor.forEach(action);
    }

    @Override
    public Spliterator<V> spliterator() {
        return this.compositeCursor.spliterator();
    }

    @Override
    public boolean hasMore() {
        return this.compositeCursor.hasMore();
    }

    @Override
    public int size() {
        return this.compositeCursor.size();
    }

    @Override
    public int totalCount() {
        return this.compositeCursor.totalCount();
    }

    @Override
    public V firstOrDefault() {
        return this.compositeCursor.firstOrDefault();
    }

    @Override
    public List<V> toList() {
        return this.compositeCursor.toList();
    }

    private Set<NitriteId> sortIdSet(Collection<NitriteId> nitriteIdSet, FindOptions findOptions) {
        String sortField = findOptions.getField();
        Collator collator = findOptions.getCollator();

        NavigableMap<Object, List<NitriteId>> sortedMap;
        if (collator != null) {
            sortedMap = new TreeMap<>(collator);
        } else {
            sortedMap = new TreeMap<>();
        }

        Set<NitriteId> nullValueIds = new HashSet<>();

        for (NitriteId id : nitriteIdSet) {
            Document document = compositeUnderlyingMap.get(id);
            if (document == null) continue;

            Object value = getFieldValue(document, sortField);

            if (value != null) {
                if (value.getClass().isArray() || value instanceof Iterable) {
                    throw new InvalidOperationException(UNABLE_TO_SORT_ON_ARRAY);
                }
            } else {
                nullValueIds.add(id);
                continue;
            }

            if (sortedMap.containsKey(value)) {
                List<NitriteId> idList = sortedMap.get(value);
                idList.add(id);
                sortedMap.put(value, idList);
            } else {
                List<NitriteId> idList = new ArrayList<>();
                idList.add(id);
                sortedMap.put(value, idList);
            }
        }

        List<NitriteId> sortedValues;
        if (findOptions.getSortOrder() == SortOrder.Ascending) {
            if (findOptions.getNullOrder() == NullOrder.Default || findOptions.getNullOrder() == NullOrder.First) {
                sortedValues = new ArrayList<>(nullValueIds);
                sortedValues.addAll(flattenList(sortedMap.values()));
            } else {
                sortedValues = flattenList(sortedMap.values());
                sortedValues.addAll(nullValueIds);
            }
        } else {
            if (findOptions.getNullOrder() == NullOrder.Default || findOptions.getNullOrder() == NullOrder.Last) {
                sortedValues = flattenList(sortedMap.descendingMap().values());
                sortedValues.addAll(nullValueIds);
            } else {
                sortedValues = new ArrayList<>(nullValueIds);
                sortedValues.addAll(flattenList(sortedMap.descendingMap().values()));
            }
        }

        return limitIdSet(sortedValues, findOptions);
    }

    private Set<NitriteId> limitIdSet(Collection<NitriteId> nitriteIdSet, FindOptions findOptions) {
        int offset = findOptions.getOffset();
        int size = findOptions.getSize();
        Set<NitriteId> resultSet = new LinkedHashSet<>();

        int index = 0;
        for (NitriteId nitriteId : nitriteIdSet) {
            if (index >= offset) {
                resultSet.add(nitriteId);
                if (index == (offset + size - 1)) break;
            }
            index++;
        }

        return resultSet;
    }

    private <T> List<T> flattenList(Collection<List<T>> collection) {
        List<T> finalList = new ArrayList<>();
        for (List<T> list : collection) {
            finalList.addAll(list);
        }
        return finalList;
    }
}
