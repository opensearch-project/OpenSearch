/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.query;

import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.NumberFieldMapper;

import java.util.ArrayList;
import java.util.List;

/**
 * A class with helper functions to construct complements for some queries.
 */
public class ComplementHelperUtils {
    /**
     * Get the NumberFieldType for this fieldName from the context, or null if it isn't a NumberFieldType.
     */
    public static NumberFieldMapper.NumberFieldType getNumberFieldType(QueryShardContext context, String fieldName) {
        if (context == null) return null;
        MappedFieldType fieldType = context.fieldMapper(fieldName);
        if (!(fieldType instanceof NumberFieldMapper.NumberFieldType nft)) return null;
        return nft;
    }

    /**
     * Returns a list of 2 RangeQueryBuilders matching everything but value.
     */
    public static List<QueryBuilder> numberValueToComplement(String fieldName, Number value) {
        List<QueryBuilder> complement = new ArrayList<>();
        RangeQueryBuilder belowRange = new RangeQueryBuilder(fieldName);
        belowRange.to(value);
        belowRange.includeUpper(false);
        complement.add(belowRange);

        RangeQueryBuilder aboveRange = new RangeQueryBuilder(fieldName);
        aboveRange.from(value);
        aboveRange.includeLower(false);
        complement.add(aboveRange);
        return complement;
    }

    /**
     * Returns a list of RangeQueryBuilders matching everything except the provided sorted values.
     * if isWholeNumber == true, and two sorted values are off by 1, the range between the two of them won't appear in
     * the complement since no value could match it.
     */
    public static List<QueryBuilder> numberValuesToComplement(String fieldName, List<Number> sortedValues, boolean isWholeNumber) {
        if (sortedValues.isEmpty()) return null;
        List<QueryBuilder> complement = new ArrayList<>();
        Number lastValue = null;
        for (Number value : sortedValues) {
            RangeQueryBuilder range = new RangeQueryBuilder(fieldName);
            range.includeUpper(false);
            range.to(value);
            if (lastValue != null) {
                // If this is a whole number field and the last value is 1 less than the current value, we can skip this part of the
                // complement
                if (isWholeNumber && value.longValue() - lastValue.longValue() == 1) {
                    lastValue = value;
                    continue;
                }
                range.includeLower(false);
                range.from(lastValue);
            }
            complement.add(range);
            lastValue = value;
        }
        // Finally add the last range query
        RangeQueryBuilder lastRange = new RangeQueryBuilder(fieldName);
        lastRange.from(sortedValues.get(sortedValues.size() - 1));
        lastRange.includeLower(false);
        lastRange.includeUpper(true);
        complement.add(lastRange);
        return complement;
    }
}
