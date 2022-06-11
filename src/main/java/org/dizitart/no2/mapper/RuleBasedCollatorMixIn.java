/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package org.dizitart.no2.mapper;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

abstract class RuleBasedCollatorMixIn {

	@JsonIgnore
	private int strength;

	@JsonCreator
	RuleBasedCollatorMixIn(@JsonProperty("rules") String rules) {
	}

	@JsonIgnore
	abstract int getDecomposition();

}
