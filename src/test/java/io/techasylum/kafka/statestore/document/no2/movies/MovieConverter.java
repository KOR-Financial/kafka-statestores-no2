package io.techasylum.kafka.statestore.document.no2.movies;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.dizitart.no2.Document;

public class MovieConverter {

    private final ObjectMapper objectMapper = new ObjectMapper();

    public MovieConverter() {
        objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    }

    public Document toDocument(Movie movie) {
        return new Document(objectMapper.convertValue(movie, new TypeReference<>() {}));
    }

    public Movie fromDocument(Document document) {
        return objectMapper.convertValue(document, new TypeReference<>() {});
    }
}
