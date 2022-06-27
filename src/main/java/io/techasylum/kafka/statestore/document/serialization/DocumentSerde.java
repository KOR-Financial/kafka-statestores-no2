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

public class DocumentSerde<Doc extends Document> implements Serde<Doc> {

	private final Serializer<Doc> serializer;
	private final Deserializer<Doc> deserializer;

	public DocumentSerde(Class<Doc> clazz, ObjectMapper objectMapper) {
		serializer = new DocumentSerializer<>(objectMapper);
		deserializer = new DocumentDeserializer<>(clazz, objectMapper);
	}

	@Override
	public Serializer<Doc> serializer() {
		return serializer;
	}

	@Override
	public Deserializer<Doc> deserializer() {
		return deserializer;
	}

}
