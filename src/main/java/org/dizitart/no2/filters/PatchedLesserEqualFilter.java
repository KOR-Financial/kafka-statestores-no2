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
import org.dizitart.no2.exceptions.FilterException;
import org.dizitart.no2.store.NitriteMap;

import static org.dizitart.no2.Constants.DOC_ID;
import static org.dizitart.no2.exceptions.ErrorCodes.FE_LTE_FIELD_NOT_COMPARABLE;
import static org.dizitart.no2.exceptions.ErrorMessage.errorMessage;
import static org.dizitart.no2.util.DocumentUtils.getFieldValue;
import static org.dizitart.no2.util.NumberUtils.compare;

class PatchedLesserEqualFilter extends ComparisonFilter {

    PatchedLesserEqualFilter(String field, Object value) {
        super(field, value);
    }

    @Override
    public Set<NitriteId> apply(NitriteMap<NitriteId, Document> documentMap) {
        if (field.equals(DOC_ID)) {
            Set<NitriteId> nitriteIdSet = new LinkedHashSet<>();
            NitriteId nitriteId = null;
            if (comparable instanceof Long) {
                nitriteId = NitriteId.createId((Long) comparable);
            }

            if (nitriteId != null) {
                NitriteId floorKey = documentMap.floorKey(nitriteId);
                while (floorKey != null) {
                    nitriteIdSet.add(floorKey);
                    floorKey = documentMap.lowerKey(floorKey);
                }
            }
            return nitriteIdSet;
        } else if (nitriteService.hasIndex(field)
                && !nitriteService.isIndexing(field)) {
            return nitriteService.findLesserEqualWithIndex(field, comparable);
        } else {
            return matchedSet(documentMap);
        }
    }

    @SuppressWarnings("unchecked")
    private Set<NitriteId> matchedSet(NitriteMap<NitriteId, Document> documentMap) {
        Set<NitriteId> nitriteIdSet = new LinkedHashSet<>();
        for (Map.Entry<NitriteId, Document> entry: documentMap.entrySet()) {
            Document document = entry.getValue();
            Object fieldValue = getFieldValue(document, field);
            if (fieldValue != null) {
                if (fieldValue instanceof Number && comparable instanceof Number) {
                    if (compare((Number) fieldValue, (Number) comparable) <= 0) {
                        nitriteIdSet.add(entry.getKey());
                    }
                } else if (fieldValue instanceof Comparable) {
                    Comparable arg = (Comparable) fieldValue;
                    if (comparable.compareTo(arg) <= 0) {
                        nitriteIdSet.add(entry.getKey());
                    }
                } else {
                    throw new FilterException(errorMessage(
                            fieldValue + " is not comparable",
                            FE_LTE_FIELD_NOT_COMPARABLE));
                }
            }
        }
        return nitriteIdSet;
    }

    @Override
    public String toString() {
        return "PatchedLesserEqualFilter{" +
                "field='" + field + '\'' +
                ", comparable=" + comparable +
                '}';
    }
}
