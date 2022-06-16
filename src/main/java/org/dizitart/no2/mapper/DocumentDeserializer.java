/*
 * Copyright 2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package org.dizitart.no2.mapper;

import java.io.IOException;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.LinkedList;
import java.util.List;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import org.dizitart.no2.Constants;
import org.dizitart.no2.Document;
import org.dizitart.no2.NitriteId;

import static org.dizitart.no2.Constants.DOC_ID;

class DocumentDeserializer extends StdDeserializer<Document> {

	private static final String DATE_REGEX = "^\\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])$";
	private static final String INSTANT_REGEX = "^\\d{4}-(0[1-9]|1[0-2])-(0[1-9]|[12][0-9]|3[01])T([01][0-9]|2[0-3]):([0-5][0-9]):([0-5][0-9])(\\.\\d{1,6})?Z$";

	DocumentDeserializer() {
		super(Document.class);
	}

	@Override
	public Document deserialize(JsonParser jsonParser, DeserializationContext ctxt)
			throws IOException {
		JsonNode node = jsonParser.getCodec().readTree(jsonParser);
		return convertJsonToRootDocument(node);
	}

	private Document convertJsonToRootDocument(JsonNode jsonNode) {
		Document document = new Document();
		jsonNode.fields().forEachRemaining((field) -> {
			if (DOC_ID.equals(field.getKey())) {
				document.put(DOC_ID, NitriteId.createId(jsonNode.get(DOC_ID).asLong()).getIdValue());
			} else {
				document.put(field.getKey(), extractValue(field.getValue()));
			}
		});
		return document;
	}

	private Document convertJsonToDocument(JsonNode jsonNode) {
		Document document = new Document();
		jsonNode.fields().forEachRemaining((field) -> document.put(field.getKey(), extractValue(field.getValue())));
		return document;
	}

	private List<Object> convertArrayNodeToList(JsonNode value) {
		List<Object> listOfValues = new LinkedList<>();
		value.elements().forEachRemaining((entry) -> listOfValues.add(extractValue(entry)));
		return listOfValues;
	}

	private Object extractValue(JsonNode jsonValue) {
		return switch (jsonValue.getNodeType()) {
		case ARRAY -> convertArrayNodeToList(jsonValue);
		case BOOLEAN -> jsonValue.asBoolean();
		case MISSING -> jsonValue.asText();
		case NULL -> null;
		case NUMBER -> jsonValue.numberValue();
		case OBJECT, POJO -> convertJsonToDocument(jsonValue);
		case STRING -> inspectText(jsonValue.asText());
		default -> throw new UnsupportedOperationException();
		};
	}

	private Object inspectText(String text) {
		if (text.matches(DATE_REGEX)) {
			return LocalDate.parse(text);
		}
		else if (text.matches(INSTANT_REGEX)) {
			return OffsetDateTime.parse(text).toInstant();
		}
		return text;
	}

}
