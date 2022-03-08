package io.techasylum.kafka.statestore.document.no2;

import org.dizitart.no2.*;
import org.dizitart.no2.exceptions.InvalidOperationException;
import org.dizitart.no2.internals.DocumentCursorInternals;
import org.dizitart.no2.store.NitriteMap;
import org.jetbrains.annotations.NotNull;

import java.text.Collator;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.counting;
import static org.dizitart.no2.exceptions.ErrorMessage.UNABLE_TO_SORT_ON_ARRAY;
import static org.dizitart.no2.internals.DocumentCursorInternals.emptyCursor;
import static org.dizitart.no2.util.DocumentUtils.getFieldValue;
import static org.dizitart.no2.util.StringUtils.isNullOrEmpty;
import static org.dizitart.no2.util.ValidationUtils.validateLimit;

public class CompositeCursor implements Cursor {

    private final Cursor compositeCursor;
    private Map<Integer, Integer> nextOffsets;
    private NitriteMap<NitriteId, Document> compositeUnderlyingMap;

    public CompositeCursor(Map<Integer, Cursor> cursorsByPartition, CompositeFindOptions compositeFindOptions) {
        Set<NitriteId> nitriteIdSet = new HashSet<>();
        for (Map.Entry<Integer, Cursor> cursorEntry : cursorsByPartition.entrySet()) {
            DocumentCursorInternals documentCursorInternals = new DocumentCursorInternals(cursorEntry.getValue());
            if (compositeUnderlyingMap == null) {
                compositeUnderlyingMap = documentCursorInternals.getUnderlyingMap();
            }
            for (Map.Entry<NitriteId, Document> entry : documentCursorInternals.getUnderlyingMap().entrySet()) {
                NitriteId newId = NitriteId.newId();
                cursorEntry.getValue().forEach((doc) -> {
                    doc.put("_id", newId);
                    doc.put("_pid", cursorEntry.getKey());
                });
                compositeUnderlyingMap.put(newId, entry.getValue());
                nitriteIdSet.add(newId);
            }
        }
        if (!nitriteIdSet.isEmpty()) {
//            validateLimit(compositeFindOptions.getFindOptionsForPartition(), nitriteIdSet.size());
            Set<NitriteId> resultSet;

            if (isNullOrEmpty(compositeFindOptions.getField())) {
                resultSet = limitIdSet(nitriteIdSet, compositeFindOptions);
            } else {
                resultSet = sortIdSet(nitriteIdSet, compositeFindOptions);
            }

            int sizeOfAllPartitions = cursorsByPartition.values().stream().mapToInt(Cursor::totalCount).sum();
            compositeCursor = DocumentCursorInternals.createCursor(resultSet, compositeUnderlyingMap, compositeFindOptions, sizeOfAllPartitions);

            nextOffsets = calculateNextOffsets(resultSet, compositeUnderlyingMap, compositeFindOptions);
        } else {
            compositeCursor = emptyCursor();
        }
    }

    private Map<Integer, Integer> calculateNextOffsets(Set<NitriteId> resultSet, NitriteMap<NitriteId, Document> compositeUnderlyingMap, CompositeFindOptions compositeFindOptions) {
        Map<Integer, Long> offsetDeltas = resultSet.stream().map((id) -> (Integer) compositeUnderlyingMap.get(id).get("_pid")).collect(Collectors.groupingBy(identity(), counting()));
        Map<Integer, Integer> offsetsByPartition = compositeFindOptions.getOffsetsByPartition();
        offsetsByPartition.replaceAll((k, v) -> offsetsByPartition.get(k) + offsetDeltas.get(k).intValue());
        return offsetsByPartition;
    }

    @Override
    public RecordIterable<Document> project(Document projection) {
        return null;
    }

    @Override
    public RecordIterable<Document> join(Cursor foreignCursor, Lookup lookup) {
        return null;
    }

    @Override
    public Set<NitriteId> idSet() {
        return null;
    }

    @NotNull
    @Override
    public Iterator<Document> iterator() {
        return this.compositeCursor.iterator();
    }

    @Override
    public void forEach(Consumer action) {
        this.compositeCursor.forEach(action);
    }

    @Override
    public Spliterator<Document> spliterator() {
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
    public Document firstOrDefault() {
        return this.compositeCursor.firstOrDefault();
    }

    @Override
    public List<Document> toList() {
        return this.compositeCursor.toList();
    }

    private Set<NitriteId> sortIdSet(Collection<NitriteId> nitriteIdSet, CompositeFindOptions findOptions) {
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

    private Set<NitriteId> limitIdSet(Collection<NitriteId> nitriteIdSet, CompositeFindOptions compositeFindOptions) {
        int offset = 0;
//        int offset = compositeFindOptions.getOffset();
        int size = compositeFindOptions.getSize();
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

    public Map<Integer, Integer> getNextOffsets() {
        return nextOffsets;
    }
}
