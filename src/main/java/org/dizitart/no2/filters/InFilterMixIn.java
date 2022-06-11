/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package org.dizitart.no2.filters;

import java.util.Set;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

abstract class InFilterMixIn extends BaseFilterMixIn {

	@JsonIgnore
	private Set<Object> objectList;

	@JsonCreator
	InFilterMixIn(@JsonProperty("field") String field, @JsonProperty("values") Object... values) {
	}

}
