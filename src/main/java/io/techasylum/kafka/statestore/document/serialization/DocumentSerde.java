/*
 * Copyright 2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package io.techasylum.kafka.statestore.document.serialization;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.dizitart.no2.Document;

public class DocumentSerde implements Serde<Document> {

	private final Serializer<Document> serializer;
	private final Deserializer<Document> deserializer;

	public DocumentSerde(ObjectMapper objectMapper) {
		serializer = new DocumentSerializer(objectMapper);
		deserializer = new DocumentDeserializer(objectMapper);
	}

	@Override
	public Serializer<Document> serializer() {
		return serializer;
	}

	@Override
	public Deserializer<Document> deserializer() {
		return deserializer;
	}

}
