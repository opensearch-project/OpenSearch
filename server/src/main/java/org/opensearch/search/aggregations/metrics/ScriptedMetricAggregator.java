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

package org.opensearch.search.aggregations.metrics;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.Scorable;
import org.apache.lucene.search.ScoreMode;
import org.opensearch.common.Nullable;
import org.opensearch.common.lease.Releasables;
import org.opensearch.common.util.ObjectArray;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.util.CollectionUtils;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptedMetricAggContexts;
import org.opensearch.script.ScriptedMetricAggContexts.MapScript;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.LeafBucketCollectorBase;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.lookup.SearchLookup;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.singletonList;

/**
 * Aggregate all docs based on a value script
 *
 * @opensearch.internal
 */
class ScriptedMetricAggregator extends MetricsAggregator {
    /**
     * Estimated cost to maintain a bucket. Since this aggregator uses
     * untracked java collections for its state it is going to both be
     * much "heavier" than a normal metric aggregator and not going to be
     * tracked by the circuit breakers properly. This is sad. So we pick a big
     * number and estimate that each bucket costs that. It could be wildly
     * inaccurate. We're sort of hoping that the real memory breaker saves
     * us here. Or that folks just don't use the aggregation.
     */
    private static final long BUCKET_COST_ESTIMATE = 1024 * 5;

    private final SearchLookup lookup;
    private final Map<String, Object> aggParams;
    @Nullable
    private final ScriptedMetricAggContexts.InitScript.Factory initScriptFactory;
    private final Map<String, Object> initScriptParams;
    private final ScriptedMetricAggContexts.MapScript.Factory mapScriptFactory;
    private final Map<String, Object> mapScriptParams;
    private final ScriptedMetricAggContexts.CombineScript.Factory combineScriptFactory;
    private final Map<String, Object> combineScriptParams;
    private final Script reduceScript;
    private ObjectArray<State> states;

    ScriptedMetricAggregator(
        String name,
        SearchLookup lookup,
        Map<String, Object> aggParams,
        @Nullable ScriptedMetricAggContexts.InitScript.Factory initScriptFactory,
        Map<String, Object> initScriptParams,
        ScriptedMetricAggContexts.MapScript.Factory mapScriptFactory,
        Map<String, Object> mapScriptParams,
        ScriptedMetricAggContexts.CombineScript.Factory combineScriptFactory,
        Map<String, Object> combineScriptParams,
        Script reduceScript,
        SearchContext context,
        Aggregator parent,
        Map<String, Object> metadata
    ) throws IOException {
        super(name, context, parent, metadata);
        this.lookup = lookup;
        this.aggParams = aggParams;
        this.initScriptFactory = initScriptFactory;
        this.initScriptParams = initScriptParams;
        this.mapScriptFactory = mapScriptFactory;
        this.mapScriptParams = mapScriptParams;
        this.combineScriptFactory = combineScriptFactory;
        this.combineScriptParams = combineScriptParams;
        this.reduceScript = reduceScript;
        states = context.bigArrays().newObjectArray(1);
    }

    @Override
    public ScoreMode scoreMode() {
        return ScoreMode.COMPLETE; // TODO: how can we know if the script relies on scores?
    }

    @Override
    public LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub) throws IOException {
        // Clear any old leaf scripts so we rebuild them on the new leaf when we first see them.
        for (long i = 0; i < states.size(); i++) {
            State state = states.get(i);
            if (state == null) {
                continue;
            }
            state.leafMapScript = null;
        }
        return new LeafBucketCollectorBase(sub, null) {
            private Scorable scorer;

            @Override
            public void setScorer(Scorable scorer) throws IOException {
                this.scorer = scorer;
            }

            @Override
            public void collect(int doc, long owningBucketOrd) throws IOException {
                states = context.bigArrays().grow(states, owningBucketOrd + 1);
                State state = states.get(owningBucketOrd);
                if (state == null) {
                    addRequestCircuitBreakerBytes(BUCKET_COST_ESTIMATE);
                    state = new State();
                    states.set(owningBucketOrd, state);
                }
                if (state.leafMapScript == null) {
                    state.leafMapScript = state.mapScript.newInstance(ctx);
                    state.leafMapScript.setScorer(scorer);
                }
                state.leafMapScript.setDocument(doc);
                state.leafMapScript.execute();
            }
        };
    }

    @Override
    public InternalAggregation buildAggregation(long owningBucketOrdinal) {
        Object result = aggStateForResult(owningBucketOrdinal).combine();
        if (result.getClass() != ScriptedAvg.class) StreamOutput.checkWriteable(result);
        return new InternalScriptedMetric(name, singletonList(result), reduceScript, metadata());
    }

    private State aggStateForResult(long owningBucketOrdinal) {
        if (owningBucketOrdinal >= states.size()) {
            return new State();
        }
        State state = states.get(owningBucketOrdinal);
        if (state == null) {
            return new State();
        }
        // The last script that touched the state at this point is the "map" script
        CollectionUtils.ensureNoSelfReferences(state.aggState, "Scripted metric aggs map script");
        return state;
    }

    @Override
    public InternalAggregation buildEmptyAggregation() {
        return new InternalScriptedMetric(name, singletonList(null), reduceScript, metadata());
    }

    @Override
    public void doClose() {
        Releasables.close(states);
    }

    private class State {
        private final ScriptedMetricAggContexts.MapScript.LeafFactory mapScript;
        private final Map<String, Object> mapScriptParamsForState;
        private final Map<String, Object> combineScriptParamsForState;
        private final Map<String, Object> aggState;
        private MapScript leafMapScript;

        State() {
            // Its possible for building the initial state to mutate the parameters as a side effect
            Map<String, Object> aggParamsForState = ScriptedMetricAggregatorFactory.deepCopyParams(aggParams, context);
            mapScriptParamsForState = ScriptedMetricAggregatorFactory.mergeParams(aggParamsForState, mapScriptParams);
            combineScriptParamsForState = ScriptedMetricAggregatorFactory.mergeParams(aggParamsForState, combineScriptParams);
            aggState = newInitialState(ScriptedMetricAggregatorFactory.mergeParams(aggParamsForState, initScriptParams));
            mapScript = mapScriptFactory.newFactory(
                ScriptedMetricAggregatorFactory.deepCopyParams(mapScriptParamsForState, context),
                aggState,
                lookup
            );
        }

        private Map<String, Object> newInitialState(Map<String, Object> initScriptParamsForState) {
            if (initScriptFactory == null) {
                return new HashMap<>();
            }
            Map<String, Object> initialState = new HashMap<>();
            initScriptFactory.newInstance(initScriptParamsForState, initialState).execute();
            CollectionUtils.ensureNoSelfReferences(initialState, "Scripted metric aggs init script");
            return initialState;
        }

        private Object combine() {
            if (combineScriptFactory == null) {
                return aggState;
            }
            Object result = combineScriptFactory.newInstance(combineScriptParamsForState, aggState).execute();
            CollectionUtils.ensureNoSelfReferences(result, "Scripted metric aggs combine script");
            return result;
        }

    }
}
