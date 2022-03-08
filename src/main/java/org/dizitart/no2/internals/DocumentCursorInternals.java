package org.dizitart.no2.internals;

import io.techasylum.kafka.statestore.document.no2.CompositeFindOptions;
import org.dizitart.no2.Document;
import org.dizitart.no2.NitriteId;
import org.dizitart.no2.store.NitriteMap;

import java.lang.reflect.Field;
import java.util.Set;

public class DocumentCursorInternals {

    private final Set<NitriteId> resultSet;
    private final NitriteMap<NitriteId, Document> underlyingMap;

    public DocumentCursorInternals(Object documentCursor) {
        Field resultSetField = null;
        Field underlyingMapField = null;
        try {
            resultSetField = DocumentCursor.class.getDeclaredField("resultSet");
            underlyingMapField = DocumentCursor.class.getDeclaredField("underlyingMap");
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
        try {
            resultSet = (Set<NitriteId>) resultSetField.get(documentCursor);
            underlyingMap = (NitriteMap<NitriteId, Document>) underlyingMapField.get(documentCursor);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }

    public static org.dizitart.no2.Cursor createCursor(Set<NitriteId> resultSet, NitriteMap<NitriteId, Document> underlyingMap, CompositeFindOptions findOptions, int sizeOfAllPartitions) {
        FindResult findResult = new FindResult();

        findResult.setIdSet(resultSet);
        // TODO: test this decently
//        findResult.setHasMore(sizeOfAllPartitions > (findOptions.getSize() + findOptions.getOffset()));
        findResult.setTotalCount(sizeOfAllPartitions);

        findResult.setIdSet(resultSet);
        findResult.setUnderlyingMap(underlyingMap);
        return new DocumentCursor(findResult);
    }

    public static org.dizitart.no2.Cursor emptyCursor() {
        return new DocumentCursor(new FindResult());
    }

    public Set<NitriteId> getResultSet() {
        return resultSet;
    }

    public NitriteMap<NitriteId, Document> getUnderlyingMap() {
        return underlyingMap;
    }
}
