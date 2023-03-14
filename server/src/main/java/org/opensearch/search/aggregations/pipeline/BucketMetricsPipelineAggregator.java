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

package org.opensearch.search.aggregations.pipeline;

import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.Aggregation;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregation.ReduceContext;
import org.opensearch.search.aggregations.InternalMultiBucketAggregation;
import org.opensearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.opensearch.search.aggregations.support.AggregationPath;

import java.util.List;
import java.util.Map;

/**
 * A class of sibling pipeline aggregations which calculate metrics across the
 * buckets of a sibling aggregation
 *
 * @opensearch.internal
 */
public abstract class BucketMetricsPipelineAggregator extends SiblingPipelineAggregator {

    protected final DocValueFormat format;
    protected final GapPolicy gapPolicy;

    BucketMetricsPipelineAggregator(
        String name,
        String[] bucketsPaths,
        GapPolicy gapPolicy,
        DocValueFormat format,
        Map<String, Object> metadata
    ) {
        super(name, bucketsPaths, metadata);
        this.gapPolicy = gapPolicy;
        this.format = format;
    }

    @Override
    public final InternalAggregation doReduce(Aggregations aggregations, ReduceContext context) {
        preCollection();
        List<String> bucketsPath = AggregationPath.parse(bucketsPaths()[0]).getPathElementsAsStringList();
        for (Aggregation aggregation : aggregations) {
            if (aggregation.getName().equals(bucketsPath.get(0))) {
                List<String> sublistedPath = bucketsPath.subList(1, bucketsPath.size());
                InternalMultiBucketAggregation<?, ?> multiBucketsAgg = (InternalMultiBucketAggregation<?, ?>) aggregation;
                List<? extends InternalMultiBucketAggregation.InternalBucket> buckets = multiBucketsAgg.getBuckets();
                for (InternalMultiBucketAggregation.InternalBucket bucket : buckets) {
                    Double bucketValue = BucketHelpers.resolveBucketValue(multiBucketsAgg, bucket, sublistedPath, gapPolicy);
                    if (bucketValue != null && !Double.isNaN(bucketValue)) {
                        collectBucketValue(bucket.getKeyAsString(), bucketValue);
                    }
                }
            }
        }
        return buildAggregation(metadata());
    }

    /**
     * Called before initial collection and between successive collection runs.
     * A chance to initialize or re-initialize state
     */
    protected void preCollection() {}

    /**
     * Called after a collection run is finished to build the aggregation for
     * the collected state.
     *
     * @param metadata
     *            the metadata to add to the resulting aggregation
     */
    protected abstract InternalAggregation buildAggregation(Map<String, Object> metadata);

    /**
     * Called for each bucket with a value so the state can be modified based on
     * the key and metric value for this bucket
     *
     * @param bucketKey
     *            the key for this bucket as a String
     * @param bucketValue
     *            the value of the metric specified in <code>bucketsPath</code>
     *            for this bucket
     */
    protected abstract void collectBucketValue(String bucketKey, Double bucketValue);
}
