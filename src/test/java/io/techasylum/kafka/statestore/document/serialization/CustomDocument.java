package io.techasylum.kafka.statestore.document.serialization;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.Instant;
import java.time.LocalDate;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import org.dizitart.no2.Document;

import static org.dizitart.no2.Constants.DOC_ID;

@JsonDeserialize(builder = CustomDocument.Builder.class)
public class CustomDocument extends Document {

	private CustomDocument() {
		super();
	}

	private CustomDocument(Map<String, Object> map) {
		super(map);
	}

	public String getFoo() {
		return get("foo", String.class);
	}

	public Boolean hasEmbeddedObject() {
		return get("hasEmbeddedObject", Boolean.class);
	}

	public EmbeddedDocument getEmbedded() {
		return get("embedded", EmbeddedDocument.class);
	}

	public List<Integer> getChildren() {
		return (List<Integer>) get("children");
	}

	public Integer getAnInt() {
		return get("anInt", Integer.class);
	}

	public BigInteger getABigInteger() {
		return get("aBigInteger", BigInteger.class);
	}

	public Long getALong() {
		return get("aLong", Long.class);
	}

	public Double getADouble() {
		return get("aDouble", Double.class);
	}

	public BigDecimal getABigDecimal() {
		return get("aBigDecimal", BigDecimal.class);
	}

	public LocalDate getADate() {
		return get("aDate", LocalDate.class);
	}

	public Instant getATimestamp() {
		return get("aTimestamp", Instant.class);
	}

	@JsonPOJOBuilder(withPrefix = "")
	public static class Builder {
		private final Map<String, Object> props = new LinkedHashMap<>();

		public Builder _id(Long id) {
			props.put(DOC_ID, id);
			return this;
		}

		public Builder foo(String foo) {
			props.put("foo", foo);
			return this;
		}

		public Builder hasEmbeddedObject(Boolean hasEmbeddedObject) {
			props.put("hasEmbeddedObject", hasEmbeddedObject);
			return this;
		}

		public Builder embedded(EmbeddedDocument embedded) {
			props.put("embedded", embedded);
			return this;
		}

		public Builder children(List<Integer> children) {
			props.put("children", children);
			return this;
		}

		public Builder anInt(Integer anInt) {
			props.put("anInt", anInt);
			return this;
		}

		public Builder aBigInteger(BigInteger aBigInteger) {
			props.put("aBigInteger", aBigInteger);
			return this;
		}

		public Builder aLong(Long aLong) {
			props.put("aLong", aLong);
			return this;
		}

		public Builder aDouble(Double aDouble) {
			props.put("aDouble", aDouble);
			return this;
		}

		public Builder aBigDecimal(BigDecimal aBigDecimal) {
			props.put("aBigDecimal", aBigDecimal);
			return this;
		}

		public Builder aDate(LocalDate aDate) {
			props.put("aDate", aDate);
			return this;
		}

		public Builder aTimestamp(Instant aTimestamp) {
			props.put("aTimestamp", aTimestamp);
			return this;
		}

		public CustomDocument build() {
			return new CustomDocument(props);
		}
	}

	@JsonDeserialize(builder = EmbeddedDocument.Builder.class)
	public static class EmbeddedDocument extends Document {

		private EmbeddedDocument() {
			super();
		}

		private EmbeddedDocument(Map<String, Object> map) {
			super(map);
		}

		public String getBar() {
			return get("bar", String.class);
		}

		@JsonPOJOBuilder(withPrefix = "")
		public static class Builder {
			private final Map<String, Object> props = new LinkedHashMap<>();

			public Builder bar(String bar) {
				props.put("bar", bar);
				return this;
			}

			public EmbeddedDocument build() {
				return new EmbeddedDocument(props);
			}
		}
	}

}
