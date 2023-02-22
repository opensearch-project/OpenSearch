/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.sort;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
import org.apache.lucene.search.SortedNumericSelector;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.comparators.NumericComparator;

import java.io.IOException;

/**
 * Sorted numeric field for wider sort types,
 * to help sorting two different numeric types.
 *
 * @opensearch.internal
 */
public class SortedWiderNumericSortField extends SortedNumericSortField {
    /**
     * Creates a sort, possibly in reverse, specifying how the sort value from the document's set is
     * selected.
     *
     * @param field    Name of field to sort by. Must not be null.
     * @param type     Type of values
     * @param reverse  True if natural order should be reversed.
     * @param selector custom selector type for choosing the sort value from the set.
     */
    public SortedWiderNumericSortField(String field, Type type, boolean reverse, SortedNumericSelector.Type selector) {
        super(field, type, reverse, selector);
    }

    /**
     * Creates and return a comparator, which always converts Numeric to double
     * and compare to support multi type comparison between numeric values
     * @param numHits number of top hits the queue will store
     * @param enableSkipping true if the comparator can skip documents via {@link
     *     LeafFieldComparator#competitiveIterator()}
     * @return NumericComparator
     */
    @Override
    public FieldComparator<?> getComparator(int numHits, boolean enableSkipping) {
        return new NumericComparator<Number>(getField(), (Number) getMissingValue(), getReverse(), enableSkipping, Double.BYTES) {
            @Override
            public int compare(int slot1, int slot2) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Number value(int slot) {
                throw new UnsupportedOperationException();
            }

            @Override
            public LeafFieldComparator getLeafComparator(LeafReaderContext context) throws IOException {
                throw new UnsupportedOperationException();
            }

            @Override
            public int compareValues(Number first, Number second) {
                if (first == null) {
                    if (second == null) {
                        return 0;
                    } else {
                        return -1;
                    }
                } else if (second == null) {
                    return 1;
                } else {
                    return Double.compare(first.doubleValue(), second.doubleValue());
                }
            }
        };
    }
}
