/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package org.dizitart.no2.filters;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;

@JsonTypeInfo(use = JsonTypeInfo.Id.NAME, property = "type")
@JsonSubTypes({ @JsonSubTypes.Type(value = AndFilter.class, name = "and"),
		@JsonSubTypes.Type(value = OrFilter.class, name = "or"),
		@JsonSubTypes.Type(value = EqualsFilter.class, name = "eq"),
		@JsonSubTypes.Type(value = PatchedEqualsFilter.class, name = "p-eq"),
		@JsonSubTypes.Type(value = NotFilter.class, name = "not"),
		@JsonSubTypes.Type(value = InFilter.class, name = "in"),
		@JsonSubTypes.Type(value = NotInFilter.class, name = "notIn"),
		@JsonSubTypes.Type(value = GreaterThanFilter.class, name = "gt"),
		@JsonSubTypes.Type(value = PatchedGreaterThanFilter.class, name = "p-gt"),
		@JsonSubTypes.Type(value = GreaterEqualFilter.class, name = "gte"),
		@JsonSubTypes.Type(value = PatchedGreaterEqualFilter.class, name = "p-gte"),
		@JsonSubTypes.Type(value = LesserThanFilter.class, name = "lt"),
		@JsonSubTypes.Type(value = PatchedLesserThanFilter.class, name = "p-lt"),
		@JsonSubTypes.Type(value = LesserEqualFilter.class, name = "lte"),
		@JsonSubTypes.Type(value = PatchedLesserEqualFilter.class, name = "p-lte"),
		@JsonSubTypes.Type(value = RegexFilter.class, name = "regex"),
		@JsonSubTypes.Type(value = TextFilter.class, name = "text"),
		@JsonSubTypes.Type(value = ElementMatchFilter.class, name = "elemMatch") })
abstract class FilterMixIn {

}
