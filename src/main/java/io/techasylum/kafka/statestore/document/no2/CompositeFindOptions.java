package io.techasylum.kafka.statestore.document.no2;

import org.dizitart.no2.FindOptions;
import org.dizitart.no2.NullOrder;
import org.dizitart.no2.SortOrder;

import java.text.Collator;
import java.util.Map;

public class CompositeFindOptions {

    private Map<Integer, Integer> offsetsByPartition;

    /**
     * Gets the number of records in each page for pagination in
     * find operation results.
     *
     * @return the record size per page.
     */
    private int size;

    /**
     * Gets the target value name for sorting the find results.
     *
     * @return the target value name.
     */
    private String field;

    /**
     * Gets the sort order of the find result.
     *
     * @return the sort order.
     */
    private SortOrder sortOrder = SortOrder.Ascending;

    /**
     * Gets the `null` values order of the find result.
     *
     * @return the `null` values order.
     */
    private NullOrder nullOrder = NullOrder.Default;

    /**
     * Gets the collator instance for sorting of {@link String}.
     *
     * @return the collator.
     */
    private Collator collator;

    /**
     * Instantiates a new find options with pagination criteria.
     *
     * @param offsetsByPartition the pagination offsets per partition.
     * @param size   the number of records per page.
     */
    public CompositeFindOptions(Map<Integer, Integer> offsetsByPartition, int size) {
        this.offsetsByPartition = offsetsByPartition;
        this.size = size;
    }

    /**
     * Instantiates a new find options with sorting criteria.
     *
     * @param field     the value to sort by.
     * @param sortOrder the sort order.
     */
    public CompositeFindOptions(String field, SortOrder sortOrder) {
        this.field = field;
        this.sortOrder = sortOrder;
    }

    /**
     * Instantiates a new find options with sorting criteria.
     *
     * @param field     the value to sort by.
     * @param sortOrder the sort order.
     * @param collator  the collator.
     */
    public CompositeFindOptions(String field, SortOrder sortOrder, Collator collator) {
        this.field = field;
        this.sortOrder = sortOrder;
        this.collator = collator;
    }

    /**
     * Instantiates a new find options with sorting criteria and `null` value order.
     *
     * @param field     the value to sort by.
     * @param sortOrder the sort order.
     * @param nullOrder the `null` value order.
     */
    public CompositeFindOptions(String field, SortOrder sortOrder, NullOrder nullOrder) {
        this.field = field;
        this.sortOrder = sortOrder;
        this.nullOrder = nullOrder;
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
        this.field = field;
        this.sortOrder = sortOrder;
        this.nullOrder = nullOrder;
        this.collator = collator;
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

    public FindOptions getFindOptionsForPartition(int partition) {
        return new FindOptions(field, sortOrder, collator, nullOrder).thenLimit(offsetsByPartition.get(partition), size);
    }

    public Integer getOffsetForPartition(int partition) {
        return offsetsByPartition.get(partition);
    }

    public Map<Integer, Integer> getOffsetsByPartition() {
        return offsetsByPartition;
    }

    public int getSize() {
        return size;
    }

    public String getField() {
        return field;
    }

    public SortOrder getSortOrder() {
        return sortOrder;
    }

    public NullOrder getNullOrder() {
        return nullOrder;
    }

    public Collator getCollator() {
        return collator;
    }
}
