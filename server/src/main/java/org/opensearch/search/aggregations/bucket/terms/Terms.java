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

package org.opensearch.search.aggregations.bucket.terms;

import org.opensearch.search.aggregations.bucket.MultiBucketsAggregation;

import java.util.List;

/**
 * A {@code terms} aggregation. Defines multiple bucket, each associated with a unique term for a specific field.
 * All documents in a bucket has the bucket's term in that field.
 *
 * @opensearch.internal
 */
public interface Terms extends MultiBucketsAggregation {

    /**
     * A bucket that is associated with a single term
     *
     * @opensearch.internal
     */
    interface Bucket extends MultiBucketsAggregation.Bucket {

        Number getKeyAsNumber();

        long getDocCountError();
    }

    /**
     * Return the sorted list of the buckets in this terms aggregation.
     */
    @Override
    List<? extends Bucket> getBuckets();

    /**
     * Get the bucket for the given term, or null if there is no such bucket.
     */
    Bucket getBucketByKey(String term);

    /**
     * Get an upper bound of the error on document counts in this aggregation.
     */
    long getDocCountError();

    /**
     * Return the sum of the document counts of all buckets that did not make
     * it to the top buckets.
     */
    long getSumOfOtherDocCounts();
}
