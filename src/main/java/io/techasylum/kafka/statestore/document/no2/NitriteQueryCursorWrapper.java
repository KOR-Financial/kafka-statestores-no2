package io.techasylum.kafka.statestore.document.no2;

import io.techasylum.kafka.statestore.document.QueryCursor;
import org.dizitart.no2.Cursor;
import org.dizitart.no2.Document;
import org.jetbrains.annotations.NotNull;

import java.util.Iterator;
import java.util.List;
import java.util.Spliterator;
import java.util.function.Consumer;

public class NitriteQueryCursorWrapper implements QueryCursor<Document> {
    private final Cursor cursor;

    public NitriteQueryCursorWrapper(Cursor cursor) {
        this.cursor = cursor;
    }

    public Cursor getCursor() {
        return cursor;
    }

    @NotNull
    @Override
    public Iterator<Document> iterator() {
        return this.cursor.iterator();
    }

    @Override
    public void forEach(Consumer<? super Document> action) {
        this.cursor.forEach(action);
    }

    @Override
    public Spliterator<Document> spliterator() {
        return this.cursor.spliterator();
    }

    @Override
    public boolean hasMore() {
        return this.cursor.hasMore();
    }

    @Override
    public int size() {
        return this.cursor.size();
    }

    @Override
    public int totalCount() {
        return this.cursor.totalCount();
    }

    @Override
    public Document firstOrDefault() {
        return this.cursor.firstOrDefault();
    }

    @Override
    public List<Document> toList() {
        return this.cursor.toList();
    }
}
