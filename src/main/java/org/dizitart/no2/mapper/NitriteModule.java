/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package org.dizitart.no2.mapper;

import java.text.Collator;
import java.text.RuleBasedCollator;

import org.dizitart.no2.Document;
import org.dizitart.no2.FindOptions;
import org.dizitart.no2.NitriteId;
import org.dizitart.no2.filters.MixInAnnotationsRegistrar;

public class NitriteModule extends NitriteIdModule {

	@Override
	public void setupModule(SetupContext context) {
		addKeySerializer(NitriteId.class, new NitriteIdKeySerializer());
		addKeyDeserializer(NitriteId.class, new NitriteIdKeyDeserializer());
		addDeserializer(Document.class, new DocumentDeserializer());
		MixInAnnotationsRegistrar.registerMixInAnnotations(context);
		setMixInAnnotation(FindOptions.class, FindOptionsMixIn.class);
		setMixInAnnotation(Collator.class, CollatorMixIn.class);
		setMixInAnnotation(RuleBasedCollator.class, RuleBasedCollatorMixIn.class);
		super.setupModule(context);
	}

}
