package io.techasylum.kafka.statestore.document.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.dizitart.no2.Document;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatObject;

class DocumentDeserializerTests {

	private DocumentDeserializer documentDeserializer = new DocumentDeserializer(new ObjectMapper().findAndRegisterModules());

	//formatter:off
	private static final String JSON = """
			{
				"_id": 805164877017594,
				"foo": "bar"
			}
		""";
	//formatter:on

	@Test
	void shouldDeserializeDocument() {
		final Document document = documentDeserializer.deserialize("my-topic", null);
		assertThatObject(document).isNotNull();
		assertThatObject(document.getId()).isEqualTo(805164877017594L);
		assertThatObject(document.get("foo")).isEqualTo("bar");
	}

	@Test
	void shouldSupportTombstone() {
		final Document document = documentDeserializer.deserialize("my-topic", null);
		assertThatObject(document).isNull();
	}

}