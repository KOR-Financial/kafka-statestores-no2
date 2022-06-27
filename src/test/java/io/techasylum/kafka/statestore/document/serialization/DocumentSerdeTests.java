package io.techasylum.kafka.statestore.document.serialization;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.dizitart.no2.Document;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatObject;

class DocumentSerdeTests {

	private DocumentSerde<Document> documentSerde = new DocumentSerde<>(Document.class, new ObjectMapper().findAndRegisterModules());

	//formatter:off
	private static final String JSON = """
			{
				"_id": 805164877017594,
				"foo": "bar",
				"hasEmbeddedObject": true,
				"embedded": {
					"bar": "baz"
				},
				"children": [
					1,
					2
				],
				"anInt": 1,
				"aBigInteger": 12345678901234567890,
				"aLong": 1234567890123456789,
				"aDouble": 11.05,
				"aBigDecimal": 12345678901234567890.0987654321,
				"aDate": "2022-06-11",
				"aTimestamp": "2022-06-11T12:58:30.396Z"
			}
		""";
	//formatter:on

	@Test
	void shouldDeserializeDocument() {
		final Document document = documentSerde.deserializer().deserialize("my-topic", JSON.getBytes());
		assertThatObject(document).isNotNull();

		// Nitrite ID
		assertThat(document.getId().getIdValue()).isEqualTo(805164877017594L);

		// String
		assertThat(document.get("foo")).isEqualTo("bar");

		// Boolean
		assertThat(document.get("hasEmbeddedObject", Boolean.class)).isTrue();

		// Embedded document
		assertThat(document.get("embedded")).isInstanceOf(LinkedHashMap.class);
		assertThat(document.get("embedded", LinkedHashMap.class).get("bar")).isEqualTo("baz");

		// List
		assertThat(document.get("children")).isInstanceOf(List.class);
		assertThat(document.get("children")).asList().containsExactly(1, 2);

		// Numbers
		assertThat(document.get("anInt", Integer.class)).isEqualTo(1);
		assertThat(document.get("aBigInteger", BigInteger.class)).isEqualTo("12345678901234567890");
		assertThat(document.get("aLong", Long.class)).isEqualTo(1234567890123456789L);
		assertThat(document.get("aDouble", Double.class)).isEqualTo(11.05);
		assertThat(document.get("aBigDecimal", Double.class)).isEqualTo(12345678901234567890.0987654321);

		// Date & time
		assertThat(document.get("aDate")).isInstanceOf(String.class);
		assertThat(document.get("aDate", String.class)).isEqualTo("2022-06-11");
		assertThat(document.get("aTimestamp")).isInstanceOf(String.class);
		assertThat(document.get("aTimestamp", String.class)).isEqualTo("2022-06-11T12:58:30.396Z");
	}

	@Test
	void shouldDeserializeFloatsToBigDecimal() {
		final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
		objectMapper.configure(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS, true);
		DocumentSerde<Document> documentSerde = new DocumentSerde<>(Document.class, objectMapper);

		final Document document = documentSerde.deserializer().deserialize("my-topic", JSON.getBytes());
		assertThat(document.get("aDouble", BigDecimal.class)).asString().isEqualTo("11.05");
		assertThat(document.get("aBigDecimal", BigDecimal.class)).asString().isEqualTo("12345678901234567890.0987654321");
	}

	@Test
	void shouldSupportTombstone() {
		final Document document = documentSerde.deserializer().deserialize("my-topic", null);
		assertThatObject(document).isNull();
	}

	@Test
	void shouldDeserializeCustomDocument() {
		final ObjectMapper objectMapper = new ObjectMapper().findAndRegisterModules();
		DocumentSerde<CustomDocument> documentSerde = new DocumentSerde<>(CustomDocument.class, objectMapper);
		final CustomDocument document = documentSerde.deserializer().deserialize("my-topic", JSON.getBytes());
		assertThatObject(document).isNotNull();

		// Nitrite ID
		assertThat(document.getId().getIdValue()).isEqualTo(805164877017594L);

		// String
		assertThat(document.getFoo()).isEqualTo("bar");

		// Boolean
		assertThat(document.hasEmbeddedObject()).isTrue();

		// Embedded document
		assertThat(document.getEmbedded().getBar()).isEqualTo("baz");

		// List
		assertThat(document.getChildren()).containsExactly(1, 2);

		// Numbers
		assertThat(document.getAnInt()).isEqualTo(1);
		assertThat(document.getABigInteger()).isEqualTo("12345678901234567890");
		assertThat(document.getALong()).isEqualTo(1234567890123456789L);
		assertThat(document.getADouble()).isEqualTo(11.05);
		assertThat(document.getABigDecimal()).asString().isEqualTo("12345678901234567890.0987654321");

		// Date & time
		assertThat(document.getADate()).isEqualTo(LocalDate.of(2022, 06, 11));
		assertThat(document.getATimestamp()).isEqualTo(Instant.parse("2022-06-11T12:58:30.396Z"));
	}

}