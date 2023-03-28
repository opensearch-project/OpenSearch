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
import org.opensearch.search.DocValueFormat;

public class BucketedSortForIntsTests extends BucketedSortTestCase<BucketedSort.ForInts> {
    @Override
    public BucketedSort.ForInts build(
        SortOrder sortOrder,
        DocValueFormat format,
        int bucketSize,
        BucketedSort.ExtraData extra,
        double[] values
    ) {
        return new BucketedSort.ForInts(bigArrays(), sortOrder, format, bucketSize, extra) {
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
                    protected int docValue() {
                        return (int) values[index];
                    }
                };
            }
        };
    }

    @Override
    protected SortValue expectedSortValue(double v) {
        return SortValue.from((long) v);
    }

    @Override
    protected double randomValue() {
        return randomIntBetween(Integer.MIN_VALUE, Integer.MAX_VALUE);
    }
}
