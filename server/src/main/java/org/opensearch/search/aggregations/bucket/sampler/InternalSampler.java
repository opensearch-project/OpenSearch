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

package org.opensearch.search.aggregations.bucket.sampler;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.aggregations.SamplingContext;
import org.opensearch.search.aggregations.bucket.InternalSingleBucketAggregation;

import java.io.IOException;
import java.util.Map;

/**
 * Implementation of sampler bucket
 *
 * @opensearch.internal
 */
public class InternalSampler extends InternalSingleBucketAggregation implements Sampler {
    public static final String NAME = "mapped_sampler";
    // InternalSampler and UnmappedSampler share the same parser name, so we use this when identifying the aggregation type
    public static final String PARSER_NAME = "sampler";

    InternalSampler(String name, long docCount, InternalAggregations subAggregations, Map<String, Object> metadata) {
        super(name, docCount, subAggregations, metadata);
    }

    /**
     * Read from a stream.
     */
    public InternalSampler(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }

    @Override
    public String getType() {
        return PARSER_NAME;
    }

    @Override
    protected InternalSingleBucketAggregation newAggregation(String name, long docCount, InternalAggregations subAggregations) {
        return new InternalSampler(name, docCount, subAggregations, metadata);
    }

    /**
     * Deliberately scales nothing, unlike every other single-bucket aggregation, and does not recurse.
     * <p>
     * This {@code doc_count} is not a count of matching documents: it is the number of documents the
     * {@link BestDocsDeferringCollector} kept, which is {@code min(shard_size, matched)} because selection is a
     * truncation by score. Scaling it by the inverse of a sampling probability would claim "we saw {@code 1 / p} times
     * as many as we kept", which says nothing and can exceed {@code shard_size}.
     * <p>
     * The same applies to everything underneath: a {@code sum} over the top {@code shard_size} documents by score is
     * not a sample of anything, so multiplying it by {@code 1 / p} does not estimate a population. Whatever sits under
     * a {@code sampler} therefore reports what it measured on the documents that were kept.
     * <p>
     * This also covers the four {@code diversified_sampler} implementations, which extend {@link SamplerAggregator} and
     * so build an {@link InternalSampler} too.
     */
    @Override
    public InternalAggregation finalizeSampling(SamplingContext samplingContext) {
        return this;
    }
}
