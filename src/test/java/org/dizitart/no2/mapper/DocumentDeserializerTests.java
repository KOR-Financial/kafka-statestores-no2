/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package org.dizitart.no2.mapper;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.dizitart.no2.Document;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DocumentDeserializerTests {

	//formatter:off
	private static final String JSON = """
			{
				"foo": "bar",
				"hasEmbeddedObject": true,
				"embedded": {
					"bar": "baz"
				},
				"children": [
					1,
					2
				],
				"int": 1,
				"bigInteger": 12345678901234567890,
				"long": 1234567890123456789,
				"double": 11.05,
				"bigDecimal": 12345678901234567890.0987654321,
				"date": "2022-06-11",
				"timestamp": "2022-06-11T12:58:30.396Z"
			}
		""";
	//formatter:on

	private ObjectMapper objectMapper;

	@BeforeEach
	void setup() {
		objectMapper = new ObjectMapper();
		objectMapper.findAndRegisterModules();
	}

	@Test
	void shouldDeserializeToDocument() throws JsonProcessingException {
		Document document = objectMapper.readValue(JSON, Document.class);

		// String
		assertThat(document.get("foo")).isEqualTo("bar");

		// Boolean
		assertThat(document.get("hasEmbeddedObject", Boolean.class)).isTrue();

		// Embedded document
		assertThat(document.get("embedded")).isInstanceOf(Document.class);
		Document embeddedDocument = document.get("embedded", Document.class);
		assertThat(embeddedDocument.get("bar")).isEqualTo("baz");

		// List
		assertThat(document.get("children")).isInstanceOf(List.class);
		assertThat(document.get("children")).asList().containsExactly(1, 2);

		// Numbers
		assertThat(document.get("int", Integer.class)).isEqualTo(1);
		assertThat(document.get("bigInteger", BigInteger.class)).isEqualTo("12345678901234567890");
		assertThat(document.get("long", Long.class)).isEqualTo(1234567890123456789L);
		assertThat(document.get("double", Double.class)).isEqualTo(11.05);
		assertThat(document.get("bigDecimal", Double.class)).isEqualTo(12345678901234567890.0987654321);

		// Date & time
		assertThat(document.get("date", LocalDate.class)).isEqualTo(LocalDate.of(2022, 06, 11));
		assertThat(document.get("timestamp", Instant.class)).isEqualTo(Instant.parse("2022-06-11T12:58:30.396Z"));
	}

	@Test
	void shouldDeserializeFloatsToBigDecimal() throws JsonProcessingException {
		objectMapper.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);

		Document document = objectMapper.readValue(JSON, Document.class);
		assertThat(document.get("double", BigDecimal.class)).asString().isEqualTo("11.05");
		assertThat(document.get("bigDecimal", BigDecimal.class)).asString().isEqualTo("12345678901234567890.0987654321");
	}

}
