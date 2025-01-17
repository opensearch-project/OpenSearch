/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree;

import org.apache.lucene.index.DocValuesType;
import org.apache.lucene.index.TermsEnum;
import org.apache.lucene.util.BytesRef;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.SortedNumericStarTreeValuesIterator;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.SortedSetStarTreeValuesIterator;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.StarTreeValuesIterator;

import java.io.IOException;
import java.util.Map;

@ExperimentalApi
public interface FieldToDimensionOrdinalMapper {

    long getMatchingOrdinal(String dimensionName, Object value, StarTreeValues starTreeValues, MatchType matchType);

    enum SingletonFactory {

        NUMERIC_FIELD_MAPPER((dimensionName, value, starTreeValues, matchType) -> {
            StarTreeValuesIterator genericIterator = starTreeValues.getDimensionValuesIterator(dimensionName);
            if (genericIterator instanceof SortedNumericStarTreeValuesIterator) {
                long parsedValue = Long.parseLong(value.toString());
                switch (matchType) {
                    case GT:
                        return parsedValue + 1;
                    case GTE:
                    case EXACT:
                    case LTE:
                        return parsedValue;
                    case LT:
                        return parsedValue - 1;
                    default:
                        return -(parsedValue - 1);
                }
            } else {
                throw new IllegalArgumentException("Unsupported star tree values iterator " + genericIterator.getClass().getName());
            }
        }),

        KEYWORD_FIELD_MAPPER((dimensionName, value, starTreeValues, matchType) -> {
            StarTreeValuesIterator genericIterator = starTreeValues.getDimensionValuesIterator(dimensionName);
            if (genericIterator instanceof SortedSetStarTreeValuesIterator) {
                SortedSetStarTreeValuesIterator sortedSetIterator = (SortedSetStarTreeValuesIterator) genericIterator;
                try {
                    BytesRef indexedValue = new BytesRef(value.toString().getBytes());
                    if (matchType == MatchType.EXACT) {
                        return sortedSetIterator.lookupTerm(indexedValue);
                    } else {
                        TermsEnum termsEnum = sortedSetIterator.termsEnum();
                        TermsEnum.SeekStatus seekStatus = termsEnum.seekCeil(indexedValue);
                        if (matchType == MatchType.GT || matchType == MatchType.GTE) {
                            return termsEnum.ord();
                        } else { // LT || LTE
                            return (seekStatus == TermsEnum.SeekStatus.NOT_FOUND) ? termsEnum.ord() - 1 : termsEnum.ord();
                        }
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            } else {
                throw new IllegalArgumentException("Unsupported star tree values iterator " + genericIterator.getClass().getName());
            }
        });

        private static final Map<String, FieldToDimensionOrdinalMapper> DOC_VALUE_TYPE_TO_MAPPER = Map.of(
            DocValuesType.SORTED_NUMERIC.name(),
            NUMERIC_FIELD_MAPPER.getFieldToDimensionOrdinalMapper(),
            DocValuesType.SORTED_SET.name(),
            KEYWORD_FIELD_MAPPER.getFieldToDimensionOrdinalMapper()
        );

        private final FieldToDimensionOrdinalMapper queryToDimensionOrdinalMapper;

        SingletonFactory(FieldToDimensionOrdinalMapper fieldToDimensionOrdinalMapper) {
            this.queryToDimensionOrdinalMapper = fieldToDimensionOrdinalMapper;
        }

        public FieldToDimensionOrdinalMapper getFieldToDimensionOrdinalMapper() {
            return queryToDimensionOrdinalMapper;
        }

        public static FieldToDimensionOrdinalMapper getFieldToDimensionOrdinalMapper(DocValuesType docValuesType) {
            return DOC_VALUE_TYPE_TO_MAPPER.get(docValuesType.name());
        }

    }

    @ExperimentalApi
    enum MatchType {
        GT,
        LT,
        GTE,
        LTE,
        EXACT
    }

}
