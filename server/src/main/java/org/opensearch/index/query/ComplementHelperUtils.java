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
}
