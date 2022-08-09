/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package org.dizitart.no2.filters;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.dizitart.no2.Filter;
import org.dizitart.no2.internals.NitriteService;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

class FilterTests {

	static ObjectMapper mapper;

	@BeforeAll
	static void setup() {
		mapper = new ObjectMapper();
		mapper.findAndRegisterModules();
	}

	@Test
	void shouldIgnoreNitriteServiceProperty() throws JsonProcessingException {
		Filter filter = Filters.in("tradeId", "some id");
		NitriteService service = mock(NitriteService.class);
		filter.setNitriteService(service);
		String json = mapper.writeValueAsString(filter);
		assertThat(json).isEqualTo("{\"type\":\"in\",\"field\":\"tradeId\",\"values\":[\"some id\"]}");
	}

	@Test
	void shouldDeserializeAndFilter() throws JsonProcessingException {
		String json = "{\"type\":\"and\",\"filters\":[]}";
		AndFilter filter = mapper.readValue(json, AndFilter.class);
		assertThat(filter.getFilters()).isEmpty();
	}

	@Test
	void shouldDeserializeOrFilter() throws JsonProcessingException {
		String json = "{\"type\":\"or\",\"filters\":[]}";
		OrFilter filter = mapper.readValue(json, OrFilter.class);
		assertThat(filter.getFilters()).isEmpty();
	}

	@Test
	void shouldDeserializeEqualsFilter() throws JsonProcessingException {
		String json = "{\"type\":\"eq\",\"field\":\"tradeId\",\"value\":\"some id\"}";
		EqualsFilter filter = mapper.readValue(json, EqualsFilter.class);
		assertThat(filter.getField()).isEqualTo("tradeId");
		assertThat(filter.getValue()).isEqualTo("some id");
	}

	@Test
	void shouldDeserializePatchedEqualsFilter() throws JsonProcessingException {
		String json = "{\"type\":\"p-eq\",\"field\":\"tradeId\",\"value\":\"some id\"}";
		PatchedEqualsFilter filter = mapper.readValue(json, PatchedEqualsFilter.class);
		assertThat(filter.getField()).isEqualTo("tradeId");
		assertThat(filter.getValue()).isEqualTo("some id");
	}

	@Test
	void shouldDeserializeNotFilter() throws JsonProcessingException {
		String json = "{\"type\":\"not\",\"filter\":{\"type\":\"eq\",\"field\":\"tradeId\",\"value\":\"some id\"}}";
		NotFilter filter = mapper.readValue(json, NotFilter.class);
		assertThat(filter.getFilter()).isNotNull();
		assertThat(filter.getFilter()).isInstanceOf(EqualsFilter.class);
	}

	@Test
	void shouldDeserializeInFilter() throws JsonProcessingException {
		String json = "{\"type\":\"in\",\"field\":\"tradeId\",\"values\":[\"some id\"]}";
		InFilter filter = mapper.readValue(json, InFilter.class);
		assertThat(filter.getField()).isEqualTo("tradeId");
		assertThat(filter.getValues()).contains("some id");
	}

	@Test
	void shouldSerializeNotInFilter() throws JsonProcessingException {
		Filter filter = Filters.notIn("tradeId", "some id");
		String json = mapper.writeValueAsString(filter);
		assertThat(json).isEqualTo("{\"type\":\"notIn\",\"field\":\"tradeId\",\"values\":[\"some id\"]}");
	}

	@Test
	void shouldDeserializeNotInFilter() throws JsonProcessingException {
		String json = "{\"type\":\"notIn\",\"field\":\"tradeId\",\"values\":[\"some id\"]}";
		NotInFilter filter = mapper.readValue(json, NotInFilter.class);
		assertThat(filter.getField()).isEqualTo("tradeId");
		assertThat(filter.getValues()).contains("some id");
	}

	@Test
	void shouldDeserializeGreaterThanFilter() throws JsonProcessingException {
		String json = "{\"type\":\"gt\",\"field\":\"tradeId\",\"comparable\":1}";
		GreaterThanFilter filter = mapper.readValue(json, GreaterThanFilter.class);
		assertThat(filter.getField()).isEqualTo("tradeId");
		assertThat(filter.getComparable()).isEqualByComparingTo(1);
	}

	@Test
	void shouldDeserializePatchedGreaterThanFilter() throws JsonProcessingException {
		String json = "{\"type\":\"p-gt\",\"field\":\"tradeId\",\"comparable\":1}";
		PatchedGreaterThanFilter filter = mapper.readValue(json, PatchedGreaterThanFilter.class);
		assertThat(filter.getField()).isEqualTo("tradeId");
		assertThat(filter.getComparable()).isEqualByComparingTo(1);
	}

	@Test
	void shouldDeserializeGreaterEqualFilter() throws JsonProcessingException {
		String json = "{\"type\":\"gte\",\"field\":\"tradeId\",\"comparable\":1}";
		GreaterEqualFilter filter = mapper.readValue(json, GreaterEqualFilter.class);
		assertThat(filter.getField()).isEqualTo("tradeId");
		assertThat(filter.getComparable()).isEqualByComparingTo(1);
	}

	@Test
	void shouldDeserializePatchedGreaterEqualFilter() throws JsonProcessingException {
		String json = "{\"type\":\"p-gte\",\"field\":\"tradeId\",\"comparable\":1}";
		PatchedGreaterEqualFilter filter = mapper.readValue(json, PatchedGreaterEqualFilter.class);
		assertThat(filter.getField()).isEqualTo("tradeId");
		assertThat(filter.getComparable()).isEqualByComparingTo(1);
	}

	@Test
	void shouldDeserializeLesserThanFilter() throws JsonProcessingException {
		String json = "{\"type\":\"lt\",\"field\":\"tradeId\",\"comparable\":1}";
		LesserThanFilter filter = mapper.readValue(json, LesserThanFilter.class);
		assertThat(filter.getField()).isEqualTo("tradeId");
		assertThat(filter.getComparable()).isEqualByComparingTo(1);
	}

	@Test
	void shouldDeserializePatchedLesserThanFilter() throws JsonProcessingException {
		String json = "{\"type\":\"p-lt\",\"field\":\"tradeId\",\"comparable\":1}";
		PatchedLesserThanFilter filter = mapper.readValue(json, PatchedLesserThanFilter.class);
		assertThat(filter.getField()).isEqualTo("tradeId");
		assertThat(filter.getComparable()).isEqualByComparingTo(1);
	}

	@Test
	void shouldDeserializeLesserEqualFilter() throws JsonProcessingException {
		String json = "{\"type\":\"lte\",\"field\":\"tradeId\",\"comparable\":1}";
		LesserEqualFilter filter = mapper.readValue(json, LesserEqualFilter.class);
		assertThat(filter.getField()).isEqualTo("tradeId");
		assertThat(filter.getComparable()).isEqualByComparingTo(1);
	}

	@Test
	void shouldDeserializePatchedLesserEqualFilter() throws JsonProcessingException {
		String json = "{\"type\":\"p-lte\",\"field\":\"tradeId\",\"comparable\":1}";
		PatchedLesserEqualFilter filter = mapper.readValue(json, PatchedLesserEqualFilter.class);
		assertThat(filter.getField()).isEqualTo("tradeId");
		assertThat(filter.getComparable()).isEqualByComparingTo(1);
	}

	@Test
	void shouldDeserializeRegexFilter() throws JsonProcessingException {
		String json = "{\"type\":\"regex\",\"field\":\"tradeId\",\"value\":\"tradeId\"}";
		RegexFilter filter = mapper.readValue(json, RegexFilter.class);
		assertThat(filter.getField()).isEqualTo("tradeId");
		assertThat(filter.getValue()).isEqualTo("tradeId");
	}

	@Test
	void shouldDeserializeTextFilter() throws JsonProcessingException {
		String json = "{\"type\":\"text\",\"field\":\"tradeId\",\"value\":\"tradeId\"}";
		TextFilter filter = mapper.readValue(json, TextFilter.class);
		assertThat(filter.getField()).isEqualTo("tradeId");
		assertThat(filter.getValue()).isEqualTo("tradeId");
	}

	@Test
	void shouldDeserializeElementMatchFilter() throws JsonProcessingException {
		String json = "{\"type\":\"elemMatch\",\"field\":\"tradeId\",\"elementFilter\":{\"type\":\"eq\",\"field\":\"tradeId\",\"value\":\"some id\"}}";
		ElementMatchFilter filter = mapper.readValue(json, ElementMatchFilter.class);
		assertThat(filter).extracting("field").isEqualTo("tradeId");
		assertThat(filter).extracting("elementFilter").isInstanceOf(EqualsFilter.class);
	}

}
