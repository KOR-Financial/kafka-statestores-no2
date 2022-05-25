package io.techasylum.kafka.statestore.document.composite;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.dizitart.no2.FindOptions;
import org.dizitart.no2.NullOrder;
import org.dizitart.no2.SortOrder;

import java.text.Collator;
import java.util.Map;

public class CompositeFindOptions extends FindOptions {

    private Map<Integer, Integer> offsetsByPartition;

    /**
     * Instantiates a new find options with pagination criteria.
     *
     * @param offsetsByPartition the pagination offsets per partition.
     * @param size   the number of records per page.
     */
    @JsonCreator
    public CompositeFindOptions(@JsonProperty("offsetsByPartition") Map<Integer, Integer> offsetsByPartition, @JsonProperty("size") int size) {
        super(0, size);
        this.offsetsByPartition = offsetsByPartition;
    }

    /**
     * Instantiates a new find options with sorting criteria.
     *
     * @param field     the value to sort by.
     * @param sortOrder the sort order.
     */
    public CompositeFindOptions(String field, SortOrder sortOrder) {
        super(field, sortOrder);
    }

    /**
     * Instantiates a new find options with sorting criteria.
     *
     * @param field     the value to sort by.
     * @param sortOrder the sort order.
     * @param collator  the collator.
     */
    public CompositeFindOptions(String field, SortOrder sortOrder, Collator collator) {
        super(field, sortOrder, collator);
    }

    /**
     * Instantiates a new find options with sorting criteria and `null` value order.
     *
     * @param field     the value to sort by.
     * @param sortOrder the sort order.
     * @param nullOrder the `null` value order.
     */
    public CompositeFindOptions(String field, SortOrder sortOrder, NullOrder nullOrder) {
        super(field, sortOrder, nullOrder);
    }

    /**
     * Instantiates a new find options with sorting criteria and `null` value order.
     *
     * @param field     the value to sort by.
     * @param sortOrder the sort order.
     * @param nullOrder the `null` value order.
     * @param collator  the collator.
     */
    public CompositeFindOptions(String field, SortOrder sortOrder, Collator collator, NullOrder nullOrder) {
        super(field, sortOrder, collator, nullOrder);
    }

    /**
     * Creates a find options with pagination criteria.
     *
     * @param offsetsByPartition the pagination offsets per partition.
     * @param size   the number of records per page.
     * @return the find options with pagination criteria.
     */
    public static CompositeFindOptions limit(Map<Integer, Integer> offsetsByPartition, int size) {
        return new CompositeFindOptions(offsetsByPartition, size);
    }

    /**
     * Creates a find options with sorting criteria.
     *
     * @param field     the value to sort by.
     * @param sortOrder the sort order.
     * @return the find options with sorting criteria.
     */
    public static CompositeFindOptions sort(String field, SortOrder sortOrder) {
        return new CompositeFindOptions(field, sortOrder);
    }

    /**
     * Creates a find options with sorting criteria.
     *
     * @param field     the value to sort by.
     * @param sortOrder the sort order.
     * @param collator  the collator.
     * @return the find options with sorting criteria.
     */
    public static CompositeFindOptions sort(String field, SortOrder sortOrder, Collator collator) {
        return new CompositeFindOptions(field, sortOrder, collator);
    }

    /**
     * Creates a find options with sorting criteria.
     *
     * @param field     the value to sort by.
     * @param sortOrder the sort order.
     * @param nullOrder the `null` value order.
     * @return the find options with sorting criteria.
     */
    public static CompositeFindOptions sort(String field, SortOrder sortOrder, NullOrder nullOrder) {
        return new CompositeFindOptions(field, sortOrder, nullOrder);
    }

    /**
     * Creates a find options with sorting criteria.
     *
     * @param field     the value to sort by.
     * @param sortOrder the sort order.
     * @param nullOrder the `null` value order.
     * @param collator  the collator.
     * @return the find options with sorting criteria.
     */
    public static CompositeFindOptions sort(String field, SortOrder sortOrder, Collator collator, NullOrder nullOrder) {
        return new CompositeFindOptions(field, sortOrder, collator, nullOrder);
    }

    /**
     * Sets the pagination criteria of a @{@link CompositeFindOptions} with sorting updateOptions.
     *
     * NOTE: With this {@link CompositeFindOptions} it will first sort all search results,
     * then it will apply pagination criteria on the sorted results.
     *
     * @param offsetsByPartition the pagination offset.
     * @param size   the number of records per page.
     * @return the find updateOptions with pagination and sorting criteria.
     */
    public CompositeFindOptions thenLimit(Map<Integer, Integer> offsetsByPartition, int size) {
        super.thenLimit(0, size);
        this.offsetsByPartition = offsetsByPartition;
        return this;
    }

    /**
     * Sets the pagination criteria of a @{@link CompositeFindOptions} with sorting updateOptions.
     *
     * NOTE: With this {@link CompositeFindOptions} it will first sort all search results,
     * then it will apply pagination criteria on the sorted results.
     *
     * @param size   the number of records per page.
     * @return the find updateOptions with pagination and sorting criteria.
     */
    public CompositeFindOptions thenLimit(int size) {
        super.thenLimit(0, size);
        return this;
    }

    public CompositeFindOptions getFindOptionsForPartition(int partition) {
        return (CompositeFindOptions) thenLimit(getOffsetForPartition(partition), getSize());
    }

    public Integer getOffsetForPartition(int partition) {
        if (offsetsByPartition != null) {
            return offsetsByPartition.get(partition);
        } else {
            return getOffset();
        }
    }

    public Map<Integer, Integer> getOffsetsByPartition() {
        return offsetsByPartition;
    }

    @Override
    public String toString() {
        return "CompositeFindOptions{" +
                "super=" + super.toString() +
                "offsetsByPartition=" + offsetsByPartition +
                '}';
    }

    public static CompositeFindOptions defaultOptions() {
        return new CompositeFindOptions(Map.of(0, 0, 1, 0), 100);
    }
}
