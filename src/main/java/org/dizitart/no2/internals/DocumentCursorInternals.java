package org.dizitart.no2.internals;

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
            resultSetField.setAccessible(true);
            underlyingMapField = DocumentCursor.class.getDeclaredField("underlyingMap");
            underlyingMapField.setAccessible(true);
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

    public Set<NitriteId> getResultSet() {
        return resultSet;
    }

    public NitriteMap<NitriteId, Document> getUnderlyingMap() {
        return underlyingMap;
    }
}
