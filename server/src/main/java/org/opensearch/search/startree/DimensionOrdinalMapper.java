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
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.SortedSetStarTreeValuesIterator;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.StarTreeValuesIterator;

import java.io.IOException;
import java.util.Map;

@ExperimentalApi
public interface DimensionOrdinalMapper {

    long getMatchingOrdinal(String dimensionName, Object value, StarTreeValues starTreeValues, MatchType matchType);

    enum SingletonFactory {

        // Casting to long ensures that all numeric fields have been converted to equivalent long at request parsing time.
        NUMERIC_FIELD_MAPPER((dimensionName, value, starTreeValues, matchType) -> (long) value),

        KEYWORD_FIELD_MAPPER((dimensionName, value, starTreeValues, matchType) -> {
            StarTreeValuesIterator genericIterator = starTreeValues.getDimensionValuesIterator(dimensionName);
            if (genericIterator instanceof SortedSetStarTreeValuesIterator) {
                SortedSetStarTreeValuesIterator sortedSetIterator = (SortedSetStarTreeValuesIterator) genericIterator;
                try {
                    if (matchType == MatchType.EXACT) {
                        return sortedSetIterator.lookupTerm((BytesRef) value);
                    } else {
                        TermsEnum termsEnum = sortedSetIterator.termsEnum();
                        TermsEnum.SeekStatus seekStatus = termsEnum.seekCeil((BytesRef) value);
                        if (matchType == MatchType.GT || matchType == MatchType.GTE) {
                            // We reached the end and couldn't match anything, else we found a term which matches.
                            return (seekStatus == TermsEnum.SeekStatus.END) ? -1 : termsEnum.ord();
                        } else { // LT || LTE
                            // If we found a term just greater, then return ordinal of the term just before it.
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

        private static final Map<String, DimensionOrdinalMapper> DOC_VALUE_TYPE_TO_MAPPER = Map.of(
            DocValuesType.SORTED_NUMERIC.name(),
            NUMERIC_FIELD_MAPPER.getFieldToDimensionOrdinalMapper(),
            DocValuesType.SORTED_SET.name(),
            KEYWORD_FIELD_MAPPER.getFieldToDimensionOrdinalMapper()
        );

        private final DimensionOrdinalMapper queryToDimensionOrdinalMapper;

        SingletonFactory(DimensionOrdinalMapper dimensionOrdinalMapper) {
            this.queryToDimensionOrdinalMapper = dimensionOrdinalMapper;
        }

        public DimensionOrdinalMapper getFieldToDimensionOrdinalMapper() {
            return queryToDimensionOrdinalMapper;
        }

        public static DimensionOrdinalMapper getFieldToDimensionOrdinalMapper(DocValuesType docValuesType) {
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
