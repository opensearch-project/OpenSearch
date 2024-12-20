/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.fielddata;

import org.apache.lucene.index.SortedNumericDocValues;
import org.opensearch.common.annotation.PublicApi;

import java.io.IOException;

/**
 * Clone of {@link SortedNumericDocValues} for unsigned long values.
 *
 * @opensearch.api
 */
@PublicApi(since = "2.19.0")
public abstract class SortedNumericUnsignedLongValues {

    /** Sole constructor. (For invocation by subclass
     * constructors, typically implicit.) */
    protected SortedNumericUnsignedLongValues() {}

    /** Advance the iterator to exactly {@code target} and return whether
     *  {@code target} has a value.
     *  {@code target} must be greater than or equal to the current
     *  doc ID and must be a valid doc ID, ie. &ge; 0 and
     *  &lt; {@code maxDoc}.*/
    public abstract boolean advanceExact(int target) throws IOException;

    /**
     * Iterates to the next value in the current document. Do not call this more than
     * {@link #docValueCount} times for the document.
     */
    public abstract long nextValue() throws IOException;

    /**
     * Retrieves the number of values for the current document.  This must always
     * be greater than zero.
     * It is illegal to call this method after {@link #advanceExact(int)}
     * returned {@code false}.
     */
    public abstract int docValueCount();

    /**
     * Advances to the first beyond the current whose document number is greater than or equal to
     * <i>target</i>, and returns the document number itself. Exhausts the iterator and returns {@link
     * org.apache.lucene.search.DocIdSetIterator#NO_MORE_DOCS} if <i>target</i> is greater than the highest document number in the set.
     *
     * This method is being used by {@link org.apache.lucene.search.comparators.NumericComparator.NumericLeafComparator} when point values optimization kicks
     * in and is implemented by most numeric types.
     */
    public int advance(int target) throws IOException {
        throw new UnsupportedOperationException();
    }

    public abstract int docID();
}
