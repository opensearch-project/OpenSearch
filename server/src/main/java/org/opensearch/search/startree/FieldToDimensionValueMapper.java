/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.startree;

import org.apache.lucene.util.BytesRef;
import org.opensearch.index.compositeindex.datacube.startree.index.StarTreeValues;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.SortedNumericStarTreeValuesIterator;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.SortedSetStarTreeValuesIterator;
import org.opensearch.index.compositeindex.datacube.startree.utils.iterator.StarTreeValuesIterator;

import java.io.IOException;

// TODO : Use this if @org.opensearch.index.compositeindex.datacube.Dimension convertToOrdinal is not sufficient.
// FIXME : Remove before deployment if unused.
public interface FieldToDimensionValueMapper {

    public long getOrdinal(String dimensionName, Object value, StarTreeValues starTreeValues);

    static enum RegisteredMapper {

        INT_FIELD_MAPPER (new FieldToDimensionValueMapper() {
            @Override
            public long getOrdinal(String dimensionName, Object value, StarTreeValues starTreeValues) {
                StarTreeValuesIterator genericIterator = starTreeValues.getDimensionValuesIterator(dimensionName);
                if (genericIterator instanceof SortedNumericStarTreeValuesIterator) {
                    return Long.parseLong(value.toString());
                } else {
                    throw new IllegalArgumentException("Unsupported star tree values iterator " + genericIterator.getClass().getName());
                }
            }
        }),

        KEYWORD_FIELD_MAPPER (new FieldToDimensionValueMapper() {
            @Override
            public long getOrdinal(String dimensionName, Object value, StarTreeValues starTreeValues) {
                StarTreeValuesIterator genericIterator = starTreeValues.getDimensionValuesIterator(dimensionName);
                if (genericIterator instanceof SortedSetStarTreeValuesIterator) {
                    try {
                        return ((SortedSetStarTreeValuesIterator) genericIterator).lookupTerm((BytesRef) value);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                } else {
                    throw new IllegalArgumentException("Unsupported star tree values iterator " + genericIterator.getClass().getName());
                }
            }
        });

        private final FieldToDimensionValueMapper fieldToDimensionValueMapper;

        RegisteredMapper(FieldToDimensionValueMapper fieldToDimensionValueMapper) {
            this.fieldToDimensionValueMapper = fieldToDimensionValueMapper;
        }

        public FieldToDimensionValueMapper getFieldToDimensionValueMapper() {
            return fieldToDimensionValueMapper;
        }

    }

}
