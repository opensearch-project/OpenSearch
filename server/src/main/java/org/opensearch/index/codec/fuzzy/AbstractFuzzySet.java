/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.fuzzy;

import org.apache.lucene.util.BytesRef;
import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.hash.T1ha1;

import java.io.IOException;
import java.util.Iterator;

/**
 * Encapsulates common behaviour implementation for a fuzzy set.
 */
public abstract class AbstractFuzzySet<T extends FuzzySet.Meta> implements FuzzySet<T> {

    /**
     * Add an item to this fuzzy set.
     * @param value The value to be added
     */
    protected abstract void add(BytesRef value);

    /**
     * Add all items to the underlying set.
     * Implementations can choose to perform this using an optimized strategy based on the type of set.
     * @param valuesIteratorProvider Supplier for an iterator over All values which should be added to the set.
     */
    protected void addAll(CheckedSupplier<Iterator<BytesRef>, IOException> valuesIteratorProvider) throws IOException {
        Iterator<BytesRef> values = valuesIteratorProvider.get();
        while (values.hasNext()) {
            add(values.next());
        }
    }

    public Result contains(BytesRef val) {
        return containsHash(generateKey(val));
    }

    protected abstract Result containsHash(long hash);

    protected long generateKey(BytesRef value) {
        return T1ha1.hash(value.bytes, value.offset, value.length, 0L);
    }

    protected void assertAllElementsExist(CheckedSupplier<Iterator<BytesRef>, IOException> iteratorProvider) throws IOException {
        Iterator<BytesRef> iter = iteratorProvider.get();
        int cnt = 0;
        while (iter.hasNext()) {
            BytesRef item = iter.next();
            assert contains(item) == Result.MAYBE
                : "Expected Filter to return positive response for elements added to it. Elements matched: " + cnt;
            cnt++;
        }
    }
}
