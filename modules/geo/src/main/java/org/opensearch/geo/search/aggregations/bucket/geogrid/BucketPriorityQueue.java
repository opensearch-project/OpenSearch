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

package org.opensearch.geo.search.aggregations.bucket.geogrid;

import org.apache.lucene.util.PriorityQueue;

/**
 * Internal priority queue for computing geogrid relations
 *
 * @opensearch.internal
 */
class BucketPriorityQueue<B extends BaseGeoGridBucket> extends PriorityQueue<B> {

    BucketPriorityQueue(int size) {
        super(size);
    }

    @Override
    protected boolean lessThan(BaseGeoGridBucket o1, BaseGeoGridBucket o2) {
        int cmp = Long.compare(o2.getDocCount(), o1.getDocCount());
        if (cmp == 0) {
            cmp = o2.compareTo(o1);
            if (cmp == 0) {
                cmp = System.identityHashCode(o2) - System.identityHashCode(o1);
            }
        }
        return cmp > 0;
    }
}
