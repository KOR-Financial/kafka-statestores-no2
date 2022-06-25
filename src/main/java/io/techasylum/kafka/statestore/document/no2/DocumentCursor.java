package io.techasylum.kafka.statestore.document.no2;

import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.dizitart.no2.Document;
import org.dizitart.no2.Lookup;
import org.dizitart.no2.NitriteId;
import org.dizitart.no2.RecordIterable;
import org.dizitart.no2.exceptions.InvalidOperationException;
import org.dizitart.no2.objects.Cursor;
import org.dizitart.no2.util.Iterables;
import org.jetbrains.annotations.NotNull;

import static org.dizitart.no2.exceptions.ErrorMessage.OBJ_REMOVE_ON_OBJECT_ITERATOR_NOT_SUPPORTED;

final class DocumentCursor<Doc extends Document> implements Cursor<Doc> {

	private final org.dizitart.no2.Cursor documentCursor;
	private final Function<Document, Doc> converter;

	DocumentCursor(org.dizitart.no2.Cursor documentCursor, Function<Document, Doc> converter) {
		this.documentCursor = documentCursor;
		this.converter = converter;
	}

	@Override
	public <P> RecordIterable<P> project(Class<P> projectionType) {
		throw new UnsupportedOperationException("Projections not yet supported");
	}

	@Override
	public <Foreign, Joined> RecordIterable<Joined> join(Cursor<Foreign> foreigns, Lookup lookup, Class<Joined> type) {
		throw new UnsupportedOperationException("Joins not yet supported");
	}

	@Override
	public Set<NitriteId> idSet() {
		return documentCursor.idSet();
	}

	@Override
	public boolean hasMore() {
		return documentCursor.hasMore();
	}

	@Override
	public int size() {
		return documentCursor.size();
	}

	@Override
	public int totalCount() {
		return documentCursor.totalCount();
	}

	@Override
	public Doc firstOrDefault() {
		return Iterables.firstOrDefault(this);
	}

	@Override
	public List<Doc> toList() {
		return Iterables.toList(this);
	}

	@NotNull
	@Override
	public Iterator<Doc> iterator() {
		return new DocumentCursorIterator(documentCursor.iterator());
	}

	private class DocumentCursorIterator implements Iterator<Doc> {
		private Iterator<Document> documentIterator;

		DocumentCursorIterator(Iterator<Document> documentIterator) {
			this.documentIterator = documentIterator;
		}

		@Override
		public boolean hasNext() {
			return documentIterator.hasNext();
		}

		@Override
		public Doc next() {
			Document document = documentIterator.next();
			if (document != null) {
				return converter.apply(document);
			}
			return null;
		}

		@Override
		public void remove() {
			throw new InvalidOperationException(OBJ_REMOVE_ON_OBJECT_ITERATOR_NOT_SUPPORTED);
		}
	}
}
