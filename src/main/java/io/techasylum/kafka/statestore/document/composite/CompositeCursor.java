package io.techasylum.kafka.statestore.document.composite;

import org.dizitart.no2.*;
import org.dizitart.no2.exceptions.InvalidOperationException;
import org.dizitart.no2.exceptions.ValidationException;
import org.dizitart.no2.internals.DocumentCursorInternals;
import org.dizitart.no2.util.Iterables;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.Collator;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.counting;
import static org.dizitart.no2.exceptions.ErrorMessage.*;
import static org.dizitart.no2.util.DocumentUtils.getFieldValue;
import static org.dizitart.no2.util.StringUtils.isNullOrEmpty;

public class CompositeCursor implements Cursor {

    private static final Logger logger = LoggerFactory.getLogger(CompositeCursor.class);

    private final Map<NitriteId, Document> compositeUnderlyingMap = new HashMap<>();
    private final Map<Integer, Integer> nextOffsets;
    private Set<NitriteId> resultSet = new HashSet<>();
    private boolean hasMore;
    private int totalCount;

    // TODO: write tests, validate, refactor
    public CompositeCursor(List<CompositeCursor> compositeCursors, CompositeFindOptions compositeFindOptions) {
        Set<NitriteId> nitriteIdSet = extractIdsAndUpdateCompositeMapFromCursors(compositeCursors);

        if (logger.isDebugEnabled()) {
            logger.debug("Extracted {} from composite cursors {} using {}", this, compositeCursors, compositeFindOptions);
        }

        if (!nitriteIdSet.isEmpty()) {
            // TODO validateLimit(compositeFindOptions.getFindOptionsForPartition(), nitriteIdSet.size());
            resultSet = nitriteIdSet;

            if (compositeFindOptions != null) {
                if (isNullOrEmpty(compositeFindOptions.getField())) {
                    resultSet = limitIdSet(nitriteIdSet, compositeFindOptions);
                } else {
                    resultSet = sortIdSet(nitriteIdSet, compositeFindOptions);
                }
            }
            totalCount = compositeCursors.stream().mapToInt(Cursor::totalCount).sum();
            compositeUnderlyingMap.keySet().retainAll(resultSet);
            hasMore = compositeCursors.parallelStream().anyMatch(RecordIterable::hasMore) || !resultSet.containsAll(nitriteIdSet);
        }
        nextOffsets = calculateNextOffsets(compositeCursors, resultSet, compositeUnderlyingMap, compositeFindOptions);

        if (logger.isDebugEnabled()) {
            logger.debug("Returning {} from composite cursors {} using {}", this, compositeCursors, compositeFindOptions);
        }
    }

    public CompositeCursor(Map<Integer, Cursor> cursorsByPartition) {
        this(cursorsByPartition, null);
    }

    public CompositeCursor(Map<Integer, Cursor> cursorsByPartition, CompositeFindOptions compositeFindOptions) {
        Set<NitriteId> nitriteIdSet = extractIdsAndUpdateCompositeMapFromCursors(cursorsByPartition);

        if (logger.isDebugEnabled()) {
            logger.debug("Extracted {} from cursors by partition {} using {}", this, cursorsByPartition, compositeFindOptions);
        }

        if (!nitriteIdSet.isEmpty()) {
        // TODO validateLimit(compositeFindOptions.getFindOptionsForPartition(), nitriteIdSet.size());
            resultSet = nitriteIdSet;

            if (compositeFindOptions != null) {
                if (isNullOrEmpty(compositeFindOptions.getField())) {
                    resultSet = limitIdSet(nitriteIdSet, compositeFindOptions);
                } else {
                    resultSet = sortIdSet(nitriteIdSet, compositeFindOptions);
                }
            }
            totalCount = cursorsByPartition.values().stream().mapToInt(Cursor::totalCount).sum();
            compositeUnderlyingMap.keySet().retainAll(resultSet);
            hasMore = cursorsByPartition.values().parallelStream().anyMatch(RecordIterable::hasMore) || !resultSet.containsAll(nitriteIdSet);
        }
        nextOffsets = calculateNextOffsets(cursorsByPartition, resultSet, compositeUnderlyingMap, compositeFindOptions);

        if (logger.isDebugEnabled()) {
            logger.debug("Returning {} from cursors by partition {} using {}", this, cursorsByPartition, compositeFindOptions);
        }
    }

    @NotNull
    private Set<NitriteId> extractIdsAndUpdateCompositeMapFromCursors(Map<Integer, Cursor> cursorsByPartition) {
        Set<NitriteId> nitriteIdSet = new HashSet<>();
        for (Map.Entry<Integer, Cursor> cursorEntry : cursorsByPartition.entrySet()) {
            DocumentCursorInternals documentCursorInternals = new DocumentCursorInternals(cursorEntry.getValue());
            for (NitriteId nitriteId : documentCursorInternals.getResultSet()) {
                NitriteId newId = NitriteId.newId();
                Document doc = documentCursorInternals.getUnderlyingMap().get(nitriteId);
                doc.put("_id", newId.getIdValue());
                doc.put("_pid", cursorEntry.getKey());
                compositeUnderlyingMap.put(newId, doc);
                nitriteIdSet.add(newId);
            }
        }
        return nitriteIdSet;
    }

    @NotNull
    private Set<NitriteId> extractIdsAndUpdateCompositeMapFromCursors(List<CompositeCursor> compositeCursors) {
        Set<NitriteId> nitriteIdSet = new HashSet<>();
        for (CompositeCursor compositeCursor : compositeCursors) {
            for (NitriteId nitriteId : compositeCursor.idSet()) {
                NitriteId newId = NitriteId.newId();
                Document doc = compositeCursor.getCompositeUnderlyingMap().get(nitriteId);
                doc.put("_id", newId.getIdValue());
                compositeUnderlyingMap.put(newId, doc);
                nitriteIdSet.add(newId);
            }
        }
        return nitriteIdSet;
    }

    private Map<Integer, Integer> calculateNextOffsets(Map<Integer, Cursor> cursorsByPartition, Set<NitriteId> resultSet, Map<NitriteId, Document> compositeUnderlyingMap, CompositeFindOptions compositeFindOptions) {
        Map<Integer, Integer> originalOffsetsByPartition = getOriginalOffsetsByPartition(cursorsByPartition, compositeFindOptions);
        Map<Integer, Long> offsetDeltas = resultSet.stream().map((id) -> (Integer) compositeUnderlyingMap.get(id).get("_pid")).collect(Collectors.groupingBy(identity(), counting()));
        Map<Integer, Integer> newOffsetsByPartition = new HashMap<>(originalOffsetsByPartition);
        for (Integer key : offsetDeltas.keySet()) {
            newOffsetsByPartition.put(key, originalOffsetsByPartition.get(key) + offsetDeltas.get(key).intValue());
        }
        return newOffsetsByPartition;
    }

    private Map<Integer, Integer> calculateNextOffsets(List<CompositeCursor> compositeCursors, Set<NitriteId> resultSet, Map<NitriteId, Document> compositeUnderlyingMap, CompositeFindOptions compositeFindOptions) {
        Map<Integer, Integer> originalOffsetsByPartition = getOriginalOffsetsByPartition(compositeCursors, compositeFindOptions);
        Map<Integer, Long> offsetDeltas = resultSet.stream().map((id) -> (Integer) compositeUnderlyingMap.get(id).get("_pid")).collect(Collectors.groupingBy(identity(), counting()));
        Map<Integer, Integer> newOffsetsByPartition = new HashMap<>(originalOffsetsByPartition);
        for (Integer key : offsetDeltas.keySet()) {
            newOffsetsByPartition.put(key, originalOffsetsByPartition.get(key) + offsetDeltas.get(key).intValue());
        }
        return newOffsetsByPartition;
    }

    private Map<Integer, Integer> getOriginalOffsetsByPartition(Map<Integer, Cursor> cursorsByPartition, CompositeFindOptions compositeFindOptions) {
        final Map<Integer, Integer> originalOffsetsByPartition;
        if (compositeFindOptions == null || compositeFindOptions.getOffsetsByPartition() == null) {
            originalOffsetsByPartition = new HashMap<>();
            cursorsByPartition.keySet().forEach((key) -> originalOffsetsByPartition.put(key, 0));
        } else {
            originalOffsetsByPartition = compositeFindOptions.getOffsetsByPartition();
        }
        return originalOffsetsByPartition;
    }

    private Map<Integer, Integer> getOriginalOffsetsByPartition(List<CompositeCursor> compositeCursors, CompositeFindOptions compositeFindOptions) {
        final Map<Integer, Integer> originalOffsetsByPartition;
        if (compositeFindOptions == null || compositeFindOptions.getOffsetsByPartition() == null) {
            originalOffsetsByPartition = compositeCursors.get(0).getNextOffsets(); // TODO: check this
        } else {
            originalOffsetsByPartition = compositeFindOptions.getOffsetsByPartition();
        }
        return originalOffsetsByPartition;
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

    @Override
    public RecordIterable<Document> project(Document projection) {
        validateProjection(projection);
//        return new ProjectedDocumentIterable(projection, findResult);
        throw new UnsupportedOperationException("Projections not yet supported");
    }

    @Override
    public RecordIterable<Document> join(Cursor cursor, Lookup lookup) {
//        return new JoinedDocumentIterable(findResult, cursor, lookup);
        throw new UnsupportedOperationException("Joins not yet supported");
    }

    @Override
    public Set<NitriteId> idSet() {
        return resultSet;
    }

    @Override
    public Iterator<Document> iterator() {
        return new CompositeCursor.DocumentCursorIterator();
    }

    @Override
    public boolean hasMore() {
        return hasMore;
    }

    @Override
    public int size() {
        return resultSet.size();
    }

    @Override
    public int totalCount() {
        return totalCount;
    }

    @Override
    public Document firstOrDefault() {
        return Iterables.firstOrDefault(this);
    }

    @Override
    public List<Document> toList() {
        return Iterables.toList(this);
    }

    private class DocumentCursorIterator implements Iterator<Document> {
        private Iterator<NitriteId> iterator;

        DocumentCursorIterator() {
            iterator = resultSet.iterator();
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public Document next() {
            NitriteId next = iterator.next();
            Document document = compositeUnderlyingMap.get(next);
            if (document != null) {
                return new Document(document);
            }
            return null;
        }

        @Override
        public void remove() {
            throw new InvalidOperationException(REMOVE_ON_DOCUMENT_ITERATOR_NOT_SUPPORTED);
        }
    }

    private void validateProjection(Document projection) {
        for (KeyValuePair kvp : projection) {
            validateKeyValuePair(kvp);
        }
    }

    private void validateKeyValuePair(KeyValuePair kvp) {
        if (kvp.getValue() != null) {
            if (!(kvp.getValue() instanceof Document)) {
                throw new ValidationException(PROJECTION_WITH_NOT_NULL_VALUES);
            } else {
                validateProjection((Document) kvp.getValue());
            }
        }
    }

    public Map<Integer, Integer> getNextOffsets() {
        return nextOffsets;
    }

    public Map<NitriteId, Document> getCompositeUnderlyingMap() {
        return compositeUnderlyingMap;
    }

    @Override
    public String toString() {
        return "CompositeCursor{" +
                "compositeUnderlyingMap=" + compositeUnderlyingMap +
                ", nextOffsets=" + nextOffsets +
                ", resultSet=" + resultSet +
                ", hasMore=" + hasMore +
                ", totalCount=" + totalCount +
                '}';
    }
}
