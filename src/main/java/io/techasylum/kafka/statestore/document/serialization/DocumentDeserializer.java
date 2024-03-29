package io.techasylum.kafka.statestore.document.serialization;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.dizitart.no2.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DocumentDeserializer<Doc extends Document> implements Deserializer<Doc> {

	private static final Logger logger = LoggerFactory.getLogger(DocumentDeserializer.class);

	private final Class<Doc> clazz;
	private final ObjectMapper objectMapper;

	DocumentDeserializer(Class<Doc> clazz, ObjectMapper objectMapper) {
		this.clazz = clazz;
		this.objectMapper = objectMapper;
	}

	@Override
	public Doc deserialize(String topic, byte[] data) {
		if (data == null) {
			return null;
		}
		try {
			return objectMapper.readValue(data, clazz);
		}
		catch (IOException ex) {
			logger.error("Could not deserialize document.", ex);
			throw new RuntimeException(ex);
		}
	}

}
