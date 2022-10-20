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

import org.opensearch.script.BucketAggregationSelectorScript;
import org.opensearch.script.Script;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.InternalAggregation.ReduceContext;
import org.opensearch.search.aggregations.InternalMultiBucketAggregation;
import org.opensearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.opensearch.search.aggregations.pipeline.BucketHelpers.resolveBucketValue;

/**
 * Aggregate all docs into a selected bucket
 *
 * @opensearch.internal
 */
public class BucketSelectorPipelineAggregator extends PipelineAggregator {
    private GapPolicy gapPolicy;
    private Script script;
    private Map<String, String> bucketsPathsMap;

    BucketSelectorPipelineAggregator(
        String name,
        Map<String, String> bucketsPathsMap,
        Script script,
        GapPolicy gapPolicy,
        Map<String, Object> metadata
    ) {
        super(name, bucketsPathsMap.values().toArray(new String[0]), metadata);
        this.bucketsPathsMap = bucketsPathsMap;
        this.script = script;
        this.gapPolicy = gapPolicy;
    }

    @Override
    public InternalAggregation reduce(InternalAggregation aggregation, ReduceContext reduceContext) {
        InternalMultiBucketAggregation<InternalMultiBucketAggregation, InternalMultiBucketAggregation.InternalBucket> originalAgg =
            (InternalMultiBucketAggregation<InternalMultiBucketAggregation, InternalMultiBucketAggregation.InternalBucket>) aggregation;
        List<? extends InternalMultiBucketAggregation.InternalBucket> buckets = originalAgg.getBuckets();

        BucketAggregationSelectorScript.Factory factory = reduceContext.scriptService()
            .compile(script, BucketAggregationSelectorScript.CONTEXT);
        List<InternalMultiBucketAggregation.InternalBucket> newBuckets = new ArrayList<>();
        for (InternalMultiBucketAggregation.InternalBucket bucket : buckets) {
            Map<String, Object> vars = new HashMap<>();
            if (script.getParams() != null) {
                vars.putAll(script.getParams());
            }
            for (Map.Entry<String, String> entry : bucketsPathsMap.entrySet()) {
                String varName = entry.getKey();
                String bucketsPath = entry.getValue();
                Double value = resolveBucketValue(originalAgg, bucket, bucketsPath, gapPolicy);
                vars.put(varName, value);
            }
            // TODO: can we use one instance of the script for all buckets? it should be stateless?
            BucketAggregationSelectorScript executableScript = factory.newInstance(vars);
            if (executableScript.execute()) {
                newBuckets.add(bucket);
            }
        }
        return originalAgg.create(newBuckets);
    }
}
