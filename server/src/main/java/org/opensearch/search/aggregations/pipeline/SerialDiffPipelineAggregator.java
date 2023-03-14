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

import org.opensearch.common.Nullable;
import org.opensearch.common.collect.EvictingQueue;
import org.opensearch.search.DocValueFormat;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregation.ReduceContext;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.InternalMultiBucketAggregation;
import org.opensearch.search.aggregations.bucket.MultiBucketsAggregation.Bucket;
import org.opensearch.search.aggregations.bucket.histogram.HistogramFactory;
import org.opensearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static org.opensearch.search.aggregations.pipeline.BucketHelpers.resolveBucketValue;

/**
 * Aggregate all docs into a serial diff bucket
 *
 * @opensearch.internal
 */
public class SerialDiffPipelineAggregator extends PipelineAggregator {
    private DocValueFormat formatter;
    private GapPolicy gapPolicy;
    private int lag;

    SerialDiffPipelineAggregator(
        String name,
        String[] bucketsPaths,
        @Nullable DocValueFormat formatter,
        GapPolicy gapPolicy,
        int lag,
        Map<String, Object> metadata
    ) {
        super(name, bucketsPaths, metadata);
        this.formatter = formatter;
        this.gapPolicy = gapPolicy;
        this.lag = lag;
    }

    @Override
    public InternalAggregation reduce(InternalAggregation aggregation, ReduceContext reduceContext) {
        InternalMultiBucketAggregation<
            ? extends InternalMultiBucketAggregation,
            ? extends InternalMultiBucketAggregation.InternalBucket> histo = (InternalMultiBucketAggregation<
                ? extends InternalMultiBucketAggregation,
                ? extends InternalMultiBucketAggregation.InternalBucket>) aggregation;
        List<? extends InternalMultiBucketAggregation.InternalBucket> buckets = histo.getBuckets();
        HistogramFactory factory = (HistogramFactory) histo;

        List<Bucket> newBuckets = new ArrayList<>();
        EvictingQueue<Double> lagWindow = new EvictingQueue<>(lag);
        int counter = 0;

        for (InternalMultiBucketAggregation.InternalBucket bucket : buckets) {
            Double thisBucketValue = resolveBucketValue(histo, bucket, bucketsPaths()[0], gapPolicy);
            Bucket newBucket = bucket;

            counter += 1;

            // Still under the initial lag period, add nothing and move on
            Double lagValue;
            if (counter <= lag) {
                lagValue = Double.NaN;
            } else {
                lagValue = lagWindow.peek();  // Peek here, because we rely on add'ing to always move the window
            }

            // Normalize null's to NaN
            if (thisBucketValue == null) {
                thisBucketValue = Double.NaN;
            }

            // Both have values, calculate diff and replace the "empty" bucket
            if (!Double.isNaN(thisBucketValue) && !Double.isNaN(lagValue)) {
                double diff = thisBucketValue - lagValue;

                List<InternalAggregation> aggs = StreamSupport.stream(bucket.getAggregations().spliterator(), false)
                    .map((p) -> (InternalAggregation) p)
                    .collect(Collectors.toList());
                aggs.add(new InternalSimpleValue(name(), diff, formatter, metadata()));
                newBucket = factory.createBucket(factory.getKey(bucket), bucket.getDocCount(), InternalAggregations.from(aggs));
            }

            newBuckets.add(newBucket);
            lagWindow.add(thisBucketValue);
        }
        return factory.createAggregation(newBuckets);
    }
}
