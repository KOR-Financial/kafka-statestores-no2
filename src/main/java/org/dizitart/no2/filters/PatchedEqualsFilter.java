/*
 * Copyright 2021-2022 KOR Financial - All Rights Reserved.
 *
 * Unauthorized copying of this file, via any medium is strictly prohibited.
 * Proprietary and confidential.
 */

package org.dizitart.no2.filters;

import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.dizitart.no2.Document;
import org.dizitart.no2.NitriteId;
import org.dizitart.no2.store.NitriteMap;

import static org.dizitart.no2.Constants.DOC_ID;
import static org.dizitart.no2.util.DocumentUtils.getFieldValue;
import static org.dizitart.no2.util.EqualsUtils.deepEquals;

/* This filter compares the input against what is stored in the database */
/* Line 62 has been adjusted to support the inversion of an equality check */
class PatchedEqualsFilter extends BaseFilter {
    private String field;
    private Object value;

    PatchedEqualsFilter(String field, Object value) {
        this.field = field;
        this.value = value;
    }

    @Override
    public Set<NitriteId> apply(NitriteMap<NitriteId, Document> documentMap) {
        if (field.equals(DOC_ID)) {
            Set<NitriteId> nitriteIdSet = new LinkedHashSet<>();
            NitriteId nitriteId = null;
            if (value instanceof Long) {
                nitriteId = NitriteId.createId((Long) value);
            }

            if (nitriteId != null) {
                if (documentMap.containsKey(nitriteId)) {
                    nitriteIdSet.add(nitriteId);
                }
            }
            return nitriteIdSet;
        } else if (nitriteService.hasIndex(field)
                && !nitriteService.isIndexing(field)
                && value != null) {
            return nitriteService.findEqualWithIndex(field, value);
        } else {
            return matchedSet(documentMap);
        }
    }

    private Set<NitriteId> matchedSet(NitriteMap<NitriteId, Document> documentMap) {
        Set<NitriteId> nitriteIdSet = new LinkedHashSet<>();
        for (Map.Entry<NitriteId, Document> entry: documentMap.entrySet()) {
            Document document = entry.getValue();
            Object fieldValue = getFieldValue(document, field);
            if (deepEquals(value, fieldValue)) {
                nitriteIdSet.add(entry.getKey());
            }
        }
        return nitriteIdSet;
    }

    public String getField() {
        return field;
    }

    public Object getValue() {
        return value;
    }

    @Override
    public String toString() {
        return "PatchedEqualsFilter{" +
                "field='" + field + '\'' +
                ", value=" + value +
                "} " + super.toString();
    }
}
