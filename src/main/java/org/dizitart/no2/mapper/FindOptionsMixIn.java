/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package org.dizitart.no2.mapper;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

abstract class FindOptionsMixIn {

	@JsonCreator
	public FindOptionsMixIn(@JsonProperty("offset") int offset, @JsonProperty("size") int size) {
	}

}
