/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.sort;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.FieldComparator;
import org.apache.lucene.search.LeafFieldComparator;
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
     */
    public SortedWiderNumericSortField(String field, Type type, boolean reverse) {
        super(field, type, reverse);
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

    /**
     * The only below types would be considered for widening during merging topDocs results for sort,
     * This will support indices having different Numeric types to be sorted together.
     * @param type SortField.Type
     * @return returns true if type is supported for widened numeric comparisons
     */
    public static boolean isTypeSupported(Type type) {
        // Only below 4 numeric types supported as of now for widened merge
        switch (type) {
            case INT:
            case LONG:
            case FLOAT:
            case DOUBLE:
                return true;
            default:
                return false;
        }
    }
}
