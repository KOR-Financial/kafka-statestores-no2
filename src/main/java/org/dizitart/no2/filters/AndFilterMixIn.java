/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package org.dizitart.no2.filters;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.dizitart.no2.Filter;

abstract class AndFilterMixIn extends BaseFilterMixIn {

	@JsonCreator
	AndFilterMixIn(@JsonProperty("filters") Filter... filters) {
	}

}
