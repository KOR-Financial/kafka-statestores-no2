/*
 *
 * Copyright 2017-2018 Nitrite author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.dizitart.no2.filters;

import org.dizitart.no2.Filter;

/**
 * A helper class to create all type of {@link Filter}s.
 *
 * @since 1.0
 * @author Anindya Chatterjee
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
