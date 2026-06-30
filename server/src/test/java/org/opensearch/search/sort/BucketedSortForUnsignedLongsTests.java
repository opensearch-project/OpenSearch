/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.sort;

import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.search.DocValueFormat;

public class BucketedSortForUnsignedLongsTests extends BucketedSortTestCase<BucketedSort.ForUnsignedLongs> {
    @Override
    public BucketedSort.ForUnsignedLongs build(
        SortOrder sortOrder,
        DocValueFormat format,
        int bucketSize,
        BucketedSort.ExtraData extra,
        double[] values
    ) {
        return new BucketedSort.ForUnsignedLongs(bigArrays(), sortOrder, format, bucketSize, extra) {
            @Override
            public Leaf forLeaf(LeafReaderContext ctx) {
                return new Leaf(ctx) {
                    int index = -1;

                    @Override
                    protected boolean advanceExact(int doc) {
                        index = doc;
                        return doc < values.length;
                    }

                    @Override
                    protected long docValue() {
                        return (long) values[index];
                    }
                };
            }
        };
    }

    @Override
    protected SortValue expectedSortValue(double v) {
        return SortValue.fromUnsigned((long) v);
    }

    @Override
    protected double randomValue() {
        return randomUnsignedLong().doubleValue();
    }

    @Override
    protected boolean isUnsignedNumeric() {
        return true;
    }
}
