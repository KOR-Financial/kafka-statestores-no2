package io.techasylum.kafka.statestore.document.serialization;

import java.io.IOException;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.dizitart.no2.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DocumentDeserializer implements Deserializer<Document> {

	private static final Logger logger = LoggerFactory.getLogger(DocumentDeserializer.class);

	private final ObjectMapper objectMapper;

	DocumentDeserializer(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	@Override
	public Document deserialize(String topic, byte[] data) {
		try {
			return objectMapper.readValue(data, Document.class);
		}
		catch (IOException ex) {
			logger.error("Could not deserialize document.", ex);
			throw new RuntimeException(ex);
		}
	}

}
