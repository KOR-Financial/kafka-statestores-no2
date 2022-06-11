package io.techasylum.kafka.statestore.document.serialization;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;
import org.dizitart.no2.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class DocumentSerializer implements Serializer<Document> {

	private static final Logger logger = LoggerFactory.getLogger(DocumentSerializer.class);

	private final ObjectMapper objectMapper;

	DocumentSerializer(ObjectMapper objectMapper) {
		this.objectMapper = objectMapper;
	}

	@Override
	public byte[] serialize(String topic, Document data) {
		try {
			return objectMapper.writeValueAsBytes(data);
		}
		catch (JsonProcessingException ex) {
			logger.error("Could not serialize document.", ex);
			throw new RuntimeException(ex);
		}
	}

}
