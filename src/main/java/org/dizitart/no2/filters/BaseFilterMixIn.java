/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package org.dizitart.no2.filters;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.dizitart.no2.internals.NitriteService;

abstract class BaseFilterMixIn {

	@JsonIgnore
	protected NitriteService nitriteService;

}
