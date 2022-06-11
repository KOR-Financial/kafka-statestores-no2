/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package org.dizitart.no2.mapper;

import java.text.ParseException;
import java.text.RuleBasedCollator;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.dizitart.no2.FindOptions;
import org.dizitart.no2.NullOrder;
import org.dizitart.no2.SortOrder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class FindOptionsTests {

	private ObjectMapper mapper;

	@BeforeEach
	void setup() {
		mapper = new ObjectMapper();
		mapper.findAndRegisterModules();
	}

	@Test
	void shouldSerializeCollator() throws JsonProcessingException, ParseException {
		FindOptions findOptions = new FindOptions("tradeId", SortOrder.Ascending,
				new RuleBasedCollator("=mocked rule"));
		String json = mapper.writeValueAsString(findOptions);
		assertThat(json).isEqualTo(
				"{\"offset\":0,\"size\":0,\"field\":\"tradeId\",\"sortOrder\":\"Ascending\",\"nullOrder\":\"Default\",\"collator\":{\"type\":\"ruleBased\",\"rules\":\"=mocked rule\"}}");
	}

	@Test
	void shouldDeserializeFindOptionsWithOffsetAndSize() throws JsonProcessingException {
		String json = "{\"offset\":1, \"size\":2}";
		FindOptions options = mapper.readValue(json, FindOptions.class);
		assertThat(options.getOffset()).isEqualTo(1);
		assertThat(options.getSize()).isEqualTo(2);
		assertThat(options.getSortOrder()).isEqualTo(SortOrder.Ascending);
		assertThat(options.getNullOrder()).isEqualTo(NullOrder.Default);
	}

	@Test
	void shouldDeserializeFindOptionsWithFieldAndSortOrder() throws JsonProcessingException {
		String json = "{\"field\":\"tradeId\", \"sortOrder\":\"Descending\"}";
		FindOptions options = mapper.readValue(json, FindOptions.class);
		assertThat(options.getOffset()).isEqualTo(0);
		assertThat(options.getSize()).isEqualTo(0);
		assertThat(options.getField()).isEqualTo("tradeId");
		assertThat(options.getSortOrder()).isEqualTo(SortOrder.Descending);
		assertThat(options.getNullOrder()).isEqualTo(NullOrder.Default);
		assertThat(options.getCollator()).isNull();
	}

	@Test
	void shouldDeserializeFindOptionsWithFieldSortOrderAndCollator() throws JsonProcessingException {
		String json = "{\"field\":\"tradeId\", \"sortOrder\":\"Descending\", \"collator\":{\"type\":\"ruleBased\",\"rules\":\"=mocked rule\"}}";
		FindOptions options = mapper.readValue(json, FindOptions.class);
		assertThat(options.getOffset()).isEqualTo(0);
		assertThat(options.getSize()).isEqualTo(0);
		assertThat(options.getField()).isEqualTo("tradeId");
		assertThat(options.getSortOrder()).isEqualTo(SortOrder.Descending);
		assertThat(options.getCollator()).isNotNull();
		assertThat(options.getCollator()).isInstanceOf(RuleBasedCollator.class);
		assertThat(options.getCollator()).extracting("rules").isEqualTo("=mocked rule");
		assertThat(options.getNullOrder()).isEqualTo(NullOrder.Default);
	}

	@Test
	void shouldDeserializeFindOptionsWithFieldSortOrderAndNullOrder() throws JsonProcessingException {
		String json = "{\"field\":\"tradeId\", \"sortOrder\":\"Descending\", \"nullOrder\":\"Last\"}";
		FindOptions options = mapper.readValue(json, FindOptions.class);
		assertThat(options.getOffset()).isEqualTo(0);
		assertThat(options.getSize()).isEqualTo(0);
		assertThat(options.getSortOrder()).isEqualTo(SortOrder.Descending);
		assertThat(options.getCollator()).isNull();
		assertThat(options.getNullOrder()).isEqualTo(NullOrder.Last);
	}

	@Test
	void shouldDeserializeFindOptionsWithFieldSortOrderCollatorAndNullOrder() throws JsonProcessingException {
		String json = "{\"field\":\"tradeId\", \"sortOrder\":\"Descending\", \"collator\":{\"type\":\"ruleBased\",\"rules\":\"=mocked rule\"}, \"nullOrder\":\"Last\"}";
		FindOptions options = mapper.readValue(json, FindOptions.class);
		assertThat(options.getOffset()).isEqualTo(0);
		assertThat(options.getSize()).isEqualTo(0);
		assertThat(options.getSortOrder()).isEqualTo(SortOrder.Descending);
		assertThat(options.getCollator()).isNotNull();
		assertThat(options.getCollator()).isInstanceOf(RuleBasedCollator.class);
		assertThat(options.getCollator()).extracting("rules").isEqualTo("=mocked rule");
		assertThat(options.getNullOrder()).isEqualTo(NullOrder.Last);
	}

}
