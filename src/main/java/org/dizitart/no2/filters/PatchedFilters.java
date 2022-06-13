/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package org.dizitart.no2.filters;

import org.dizitart.no2.Filter;

/**
 * A helper class to create all type of patched {@link Filter}s.
 */
public class PatchedFilters {

    private PatchedFilters() {

    }

    /**
     * Creates a greater than filter which matches those documents where the value
     * of the value is greater than (i.e. >) the specified value.
     *
     * [[app-listing]]
     * [source,java]
     * .Example
     * --
     * // matches all documents where 'age' field has value greater than 30
     * collection.find(gt("age", 30));
     * --
     *
     * @param field the value
     * @param value the value
     * @return the greater than filter
     */
    public static Filter gt(String field, Object value) {
        return new PatchedGreaterThanFilter(field, value);
    }

    /**
     * Creates a greater equal filter which matches those documents where the value
     * of the value is greater than or equals to (i.e. >=) the specified value.
     *
     * [[app-listing]]
     * [source,java]
     * .Example
     * --
     * // matches all documents where 'age' field has value greater than or equal to 30
     * collection.find(gte("age", 30));
     * --
     *
     * @param field the value
     * @param value the value
     * @return the greater or equal filter
     */
    public static Filter gte(String field, Object value) {
        return new PatchedGreaterEqualFilter(field, value);
    }

    /**
     * Creates a lesser than filter which matches those documents where the value
     * of the value is less than (i.e. <) the specified value.
     *
     * [[app-listing]]
     * [source,java]
     * .Example
     * --
     * // matches all documents where 'age' field has value less than 30
     * collection.find(lt("age", 30));
     * --
     *
     * @param field the value
     * @param value the value
     * @return the lesser than filter
     */
    public static Filter lt(String field, Object value) {
        return new PatchedLesserThanFilter(field, value);
    }

    /**
     * Creates a lesser equal filter which matches those documents where the value
     * of the value is lesser than or equals to (i.e. <=) the specified value.
     *
     * [[app-listing]]
     * [source,java]
     * .Example
     * --
     * // matches all documents where 'age' field has value lesser than or equal to 30
     * collection.find(lte("age", 30));
     * --
     *
     * @param field the value
     * @param value the value
     * @return the lesser equal filter
     */
    public static Filter lte(String field, Object value) {
        return new PatchedLesserEqualFilter(field, value);
    }

}
