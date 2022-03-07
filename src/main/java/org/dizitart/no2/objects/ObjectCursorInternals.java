package org.dizitart.no2.objects;

import io.techasylum.kafka.statestore.document.no2.NitriteQueryCursorWrapper;
import org.dizitart.no2.Cursor;

import java.lang.reflect.Field;

public class ObjectCursorInternals {

    private final org.dizitart.no2.Cursor internalCursor;

    public <V> ObjectCursorInternals(NitriteQueryCursorWrapper<V> cursor) {
        ObjectCursor<V> objectCursor = (ObjectCursor<V>) cursor.getCursor();
        Field cursorField = null;
        try {
            cursorField = ObjectCursor.class.getDeclaredField("cursor");
        } catch (NoSuchFieldException e) {
            e.printStackTrace();
        }
        try {
            internalCursor = (org.dizitart.no2.Cursor) cursorField.get(objectCursor);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException(e);
        }
    }

    public static <V> org.dizitart.no2.objects.Cursor<V> createCursor(Cursor cursor) {
        return new ObjectCursor<>(null, cursor, null);
    }

    public Cursor getInternalCursor() {
        return internalCursor;
    }
}
