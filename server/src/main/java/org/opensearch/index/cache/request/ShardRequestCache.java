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

package org.opensearch.index.cache.request;

import org.apache.lucene.util.Accountable;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.indices.TierType;

import java.util.EnumMap;

/**
 * Tracks the portion of the request cache in use for a particular shard.
 *
 * @opensearch.internal
 */
public final class ShardRequestCache {

    private EnumMap<TierType, StatsHolder> statsHolder = new EnumMap<>(TierType.class);

    public ShardRequestCache() {
        for (TierType tierType : TierType.values()) {
            statsHolder.put(tierType, new StatsHolder());
        }
    }

    public RequestCacheStats stats() {
        // TODO: Change RequestCacheStats to support disk tier stats.
        return new RequestCacheStats(
            statsHolder.get(TierType.ON_HEAP).totalMetric.count(),
            statsHolder.get(TierType.ON_HEAP).evictionsMetric.count(),
            statsHolder.get(TierType.ON_HEAP).hitCount.count(),
            statsHolder.get(TierType.ON_HEAP).missCount.count()
        );
    }

    public void onHit(TierType tierType) {
        statsHolder.get(tierType).hitCount.inc();
    }

    public void onMiss(TierType tierType) {
        statsHolder.get(tierType).missCount.inc();
    }

    public void onCached(Accountable key, BytesReference value, TierType tierType) {
        statsHolder.get(tierType).totalMetric.inc(key.ramBytesUsed() + value.ramBytesUsed());
    }

    public void onRemoval(Accountable key, BytesReference value, boolean evicted, TierType tierType) {
        if (evicted) {
            statsHolder.get(tierType).evictionsMetric.inc();
        }
        long dec = 0;
        if (key != null) {
            dec += key.ramBytesUsed();
        }
        if (value != null) {
            dec += value.ramBytesUsed();
        }
        statsHolder.get(tierType).totalMetric.dec(dec);
    }

    static class StatsHolder {

        final CounterMetric evictionsMetric = new CounterMetric();
        final CounterMetric totalMetric = new CounterMetric();
        final CounterMetric hitCount = new CounterMetric();
        final CounterMetric missCount = new CounterMetric();
    }
}
