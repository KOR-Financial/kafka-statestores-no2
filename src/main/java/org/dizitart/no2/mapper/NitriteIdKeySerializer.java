/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package org.dizitart.no2.mapper;

import java.io.IOException;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.ser.std.StdScalarSerializer;
import org.dizitart.no2.NitriteId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

class NitriteIdKeySerializer extends StdScalarSerializer<NitriteId> {

	private static final Logger logger = LoggerFactory.getLogger(NitriteIdKeySerializer.class);

	protected NitriteIdKeySerializer() {
		super(NitriteId.class);
	}

	@Override
	public void serialize(NitriteId value, JsonGenerator gen, SerializerProvider provider) throws IOException {
		if (value.getIdValue() == null) {
			logger.error("Could not serialize NitriteId {}", value);
			throw new IllegalStateException("Could not serialize NitriteId " + value);
		}
		gen.writeFieldName(value.getIdValue().toString());
	}

}
