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

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregation.ReduceContext;
import org.opensearch.search.aggregations.InternalAggregations;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Aggregate all docs into a sibling bucket
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public abstract class SiblingPipelineAggregator extends PipelineAggregator {
    protected SiblingPipelineAggregator(String name, String[] bucketsPaths, Map<String, Object> metadata) {
        super(name, bucketsPaths, metadata);
    }

    /**
     * Read from a stream.
     */
    SiblingPipelineAggregator(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public InternalAggregation reduce(InternalAggregation aggregation, ReduceContext reduceContext) {
        return aggregation.copyWithRewritenBuckets(aggregations -> {
            List<InternalAggregation> aggs = aggregations.copyResults();
            aggs.add(doReduce(aggregations, reduceContext));
            return InternalAggregations.from(aggs);
        });
    }

    public abstract InternalAggregation doReduce(Aggregations aggregations, ReduceContext context);
}
