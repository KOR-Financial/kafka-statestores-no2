package io.techasylum.kafka.statestore.document.composite;

import java.text.Collator;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.dizitart.no2.Cursor;
import org.dizitart.no2.Document;
import org.dizitart.no2.KeyValuePair;
import org.dizitart.no2.Lookup;
import org.dizitart.no2.NitriteId;
import org.dizitart.no2.NullOrder;
import org.dizitart.no2.RecordIterable;
import org.dizitart.no2.SortOrder;
import org.dizitart.no2.exceptions.InvalidOperationException;
import org.dizitart.no2.exceptions.ValidationException;
import org.dizitart.no2.internals.DocumentCursorInternals;
import org.dizitart.no2.util.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.function.Function.identity;
import static java.util.stream.Collectors.counting;
import static org.dizitart.no2.exceptions.ErrorMessage.PROJECTION_WITH_NOT_NULL_VALUES;
import static org.dizitart.no2.exceptions.ErrorMessage.REMOVE_ON_DOCUMENT_ITERATOR_NOT_SUPPORTED;
import static org.dizitart.no2.exceptions.ErrorMessage.UNABLE_TO_SORT_ON_ARRAY;
import static org.dizitart.no2.util.DocumentUtils.getFieldValue;
import static org.dizitart.no2.util.StringUtils.isNullOrEmpty;

public class CompositeCursor implements Cursor {

    private static final Logger logger = LoggerFactory.getLogger(CompositeCursor.class);

    @JsonProperty
    private final Map<NitriteId, Document> documents;

    @JsonProperty
    private final Map<Integer, Integer> nextOffsets;

    @JsonProperty
    private final Set<NitriteId> resultSet;

    @JsonProperty
    private final boolean hasMore;

    @JsonProperty
    private final int totalCount;

    @JsonCreator
    private CompositeCursor(@JsonProperty("documents") Map<NitriteId, Document> documents, @JsonProperty("resultSet") Set<NitriteId> resultSet,
            @JsonProperty("nextOffsets") Map<Integer, Integer> nextOffsets, @JsonProperty("hasMore") boolean hasMore, @JsonProperty("totalCount") int totalCount) {
        this.documents = documents;
        this.nextOffsets = nextOffsets;
        this.resultSet = resultSet;
        this.hasMore = hasMore;
        this.totalCount = totalCount;
    }

    public Map<NitriteId, Document> documents() {
        return Collections.unmodifiableMap(documents);
    }

    public Map<Integer, Integer> nextOffsets() {
        return Collections.unmodifiableMap(nextOffsets);
    }

    public Set<NitriteId> resultSet() {
        return Collections.unmodifiableSet(resultSet);
    }

    @Override
    public boolean hasMore() {
        return hasMore;
    }

    @Override
    public int totalCount() {
        return totalCount;
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
    public int size() {
        return resultSet.size();
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
            Document document = documents.get(next);
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

    @Override
    public String toString() {
        return "CompositeCursor{" +
                "documents=" + documents +
                ", nextOffsets=" + nextOffsets +
                ", resultSet=" + resultSet +
                ", hasMore=" + hasMore +
                ", totalCount=" + totalCount +
                '}';
    }

    // TODO: write tests, validate, refactor
    public static CompositeCursor of(List<CompositeCursor> compositeCursors, CompositeFindOptions compositeFindOptions) {
        Map<NitriteId, Document> documents = extractDocumentsFromCursors(compositeCursors);
        Set<NitriteId> nitriteIdSet = documents.keySet();

        int totalCount = 0;
        boolean hasMore = false;
        if (!nitriteIdSet.isEmpty()) {
            // TODO validateLimit(compositeFindOptions.getFindOptionsForPartition(), nitriteIdSet.size());
            Set<NitriteId> originalNitriteIdSet = new HashSet<>(nitriteIdSet);
            if (compositeFindOptions != null) {
                if (isNullOrEmpty(compositeFindOptions.getField())) {
                    nitriteIdSet = limitDocuments(documents, compositeFindOptions);
                } else {
                    nitriteIdSet = sortDocuments(documents, compositeFindOptions);
                }
            }
            totalCount = compositeCursors.stream().mapToInt(Cursor::totalCount).sum();
            documents.keySet().retainAll(nitriteIdSet);
            hasMore = compositeCursors.parallelStream().anyMatch(RecordIterable::hasMore) || !nitriteIdSet.containsAll(originalNitriteIdSet);
        }
        Map<Integer, Integer> nextOffsets = calculateNextOffsets(compositeCursors, nitriteIdSet, documents, compositeFindOptions);

        CompositeCursor cursor = new CompositeCursor(documents, nitriteIdSet, nextOffsets, hasMore, totalCount);
        if (logger.isDebugEnabled()) {
            logger.debug("Returning {} from composite cursors {} using {}", cursor, compositeCursors, compositeFindOptions);
        }
        return cursor;
    }

    public static CompositeCursor of(Map<Integer, Cursor> cursorsByPartition) {
        return CompositeCursor.of(cursorsByPartition, null);
    }

    public static CompositeCursor of(Map<Integer, Cursor> cursorsByPartition, CompositeFindOptions compositeFindOptions) {
        Map<NitriteId, Document> documents = extractDocumentsFromCursors(cursorsByPartition);
        Set<NitriteId> nitriteIdSet = documents.keySet();

        int totalCount = 0;
        boolean hasMore = false;
        if (!nitriteIdSet.isEmpty()) {
            // TODO validateLimit(compositeFindOptions.getFindOptionsForPartition(), nitriteIdSet.size());
            Set<NitriteId> originalNitriteIdSet = new HashSet<>(nitriteIdSet);

            if (compositeFindOptions != null) {
                if (isNullOrEmpty(compositeFindOptions.getField())) {
                    nitriteIdSet = limitDocuments(originalNitriteIdSet, compositeFindOptions);
                } else {
                    nitriteIdSet = sortDocuments(documents, compositeFindOptions);
                }
            }
            totalCount = cursorsByPartition.values().stream().mapToInt(Cursor::totalCount).sum();
            documents.keySet().retainAll(nitriteIdSet);
            hasMore = cursorsByPartition.values().parallelStream().anyMatch(RecordIterable::hasMore) || !nitriteIdSet.containsAll(originalNitriteIdSet);
        }
        Map<Integer, Integer> nextOffsets = calculateNextOffsets(cursorsByPartition, nitriteIdSet, documents, compositeFindOptions);

        CompositeCursor cursor = new CompositeCursor(documents, nitriteIdSet, nextOffsets, hasMore, totalCount);
        if (logger.isDebugEnabled()) {
            logger.debug("Returning {} from cursors by partition {} using {}", cursor, cursorsByPartition, compositeFindOptions);
        }
        return cursor;
    }

    private static Map<NitriteId, Document> extractDocumentsFromCursors(List<CompositeCursor> compositeCursors) {
        Map<NitriteId, Document> documents = new HashMap<>();
        for (CompositeCursor compositeCursor : compositeCursors) {
            for (NitriteId nitriteId : compositeCursor.idSet()) {
                NitriteId newId = NitriteId.newId();
                Document doc = new Document(compositeCursor.documents().get(nitriteId));
                doc.put("_id", newId.getIdValue());
                documents.put(newId, doc);
            }
        }
        return documents;
    }

    private static Map<NitriteId, Document> extractDocumentsFromCursors(Map<Integer, Cursor> cursorsByPartition) {
        Map<NitriteId, Document> documents = new HashMap<>();
        for (Map.Entry<Integer, Cursor> cursorEntry : cursorsByPartition.entrySet()) {
            DocumentCursorInternals documentCursorInternals = new DocumentCursorInternals(cursorEntry.getValue());
            for (NitriteId nitriteId : documentCursorInternals.getResultSet()) {
                NitriteId newId = NitriteId.newId();
                Document doc = new Document(documentCursorInternals.getUnderlyingMap().get(nitriteId));
                doc.put("_id", newId.getIdValue());
                doc.put("_pid", cursorEntry.getKey());
                documents.put(newId, doc);
            }
        }
        return documents;
    }

    private static Map<Integer, Integer> calculateNextOffsets(Map<Integer, Cursor> cursorsByPartition, Set<NitriteId> resultSet, Map<NitriteId, Document> compositeUnderlyingMap, CompositeFindOptions compositeFindOptions) {
        Map<Integer, Integer> originalOffsetsByPartition = getOriginalOffsetsByPartition(cursorsByPartition, compositeFindOptions);
        Map<Integer, Long> offsetDeltas = resultSet.stream().map((id) -> (Integer) compositeUnderlyingMap.get(id).get("_pid")).collect(Collectors.groupingBy(identity(), counting()));
        Map<Integer, Integer> newOffsetsByPartition = new HashMap<>(originalOffsetsByPartition);
        for (Integer key : offsetDeltas.keySet()) {
            Integer originalOffset = originalOffsetsByPartition.get(key);
            if (originalOffset == null) {
                originalOffset = 0;
            }
            newOffsetsByPartition.put(key, originalOffset + offsetDeltas.get(key).intValue());
        }
        return newOffsetsByPartition;
    }

    private static Map<Integer, Integer> calculateNextOffsets(List<CompositeCursor> compositeCursors, Set<NitriteId> resultSet, Map<NitriteId, Document> compositeUnderlyingMap, CompositeFindOptions compositeFindOptions) {
        Map<Integer, Integer> originalOffsetsByPartition = getOriginalOffsetsByPartition(compositeCursors, compositeFindOptions);
        Map<Integer, Long> offsetDeltas = resultSet.stream().map((id) -> (Integer) compositeUnderlyingMap.get(id).get("_pid")).collect(Collectors.groupingBy(identity(), counting()));
        Map<Integer, Integer> newOffsetsByPartition = new HashMap<>(originalOffsetsByPartition);
        for (Integer key : offsetDeltas.keySet()) {
            Integer originalOffset = originalOffsetsByPartition.get(key);
            if (originalOffset == null) {
                originalOffset = 0;
            }
            newOffsetsByPartition.put(key, originalOffset + offsetDeltas.get(key).intValue());
        }
        return newOffsetsByPartition;
    }

    private static Map<Integer, Integer> getOriginalOffsetsByPartition(Map<Integer, Cursor> cursorsByPartition, CompositeFindOptions compositeFindOptions) {
        final Map<Integer, Integer> originalOffsetsByPartition;
        if (compositeFindOptions == null || compositeFindOptions.getOffsetsByPartition() == null) {
            originalOffsetsByPartition = new HashMap<>();
            cursorsByPartition.keySet().forEach((key) -> originalOffsetsByPartition.put(key, 0));
        } else {
            originalOffsetsByPartition = compositeFindOptions.getOffsetsByPartition();
        }
        return originalOffsetsByPartition;
    }

    private static Map<Integer, Integer> getOriginalOffsetsByPartition(List<CompositeCursor> compositeCursors, CompositeFindOptions compositeFindOptions) {
        final Map<Integer, Integer> originalOffsetsByPartition;
        if (compositeFindOptions == null || compositeFindOptions.getOffsetsByPartition() == null) {
            originalOffsetsByPartition = compositeCursors.get(0).nextOffsets(); // TODO: check this
        } else {
            originalOffsetsByPartition = compositeFindOptions.getOffsetsByPartition();
        }
        return originalOffsetsByPartition;
    }

    private static Set<NitriteId> sortDocuments(Map<NitriteId, Document> documents, CompositeFindOptions findOptions) {
        Collection<NitriteId> nitriteIdSet = documents.keySet();
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
            Document document = documents.get(id);
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

        return limitDocuments(sortedValues, findOptions);
    }

    // TODO refactor
    private static Set<NitriteId> limitDocuments(Map<NitriteId, Document> documents, CompositeFindOptions compositeFindOptions) {
        return limitDocuments(documents.keySet(), compositeFindOptions);
    }

    private static Set<NitriteId> limitDocuments(Collection<NitriteId> nitriteIdSet, CompositeFindOptions compositeFindOptions) {
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

    private static <T> List<T> flattenList(Collection<List<T>> collection) {
        List<T> finalList = new ArrayList<>();
        for (List<T> list : collection) {
            finalList.addAll(list);
        }
        return finalList;
    }
}
