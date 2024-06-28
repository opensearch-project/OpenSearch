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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.Accountable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.core.common.bytes.BytesReference;

/**
 * Tracks the portion of the request cache in use for a particular shard.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class ShardRequestCache {

    private static final Logger logger = LogManager.getLogger(ShardRequestCache.class);
    final CounterMetric evictionsMetric = new CounterMetric();
    final CounterMetric totalMetric = new CounterMetric();
    final CounterMetric hitCount = new CounterMetric();
    final CounterMetric missCount = new CounterMetric();

    public RequestCacheStats stats() {
        return new RequestCacheStats(Math.max(0, totalMetric.count()), evictionsMetric.count(), hitCount.count(), missCount.count());
    }

    public void onHit() {
        hitCount.inc();
    }

    public void onMiss() {
        missCount.inc();
    }

    // Functions used to increment size by passing in the size directly, Used now, as we use ICacheKey<Key> in the IndicesRequestCache..
    public void onCached(long keyRamBytesUsed, BytesReference value) {
        totalMetric.inc(keyRamBytesUsed + value.ramBytesUsed());
    }

    public void onRemoval(long keyRamBytesUsed, BytesReference value, boolean evicted) {
        if (evicted) {
            evictionsMetric.inc();
        }
        long dec = keyRamBytesUsed;
        if (value != null) {
            dec += value.ramBytesUsed();
        }
        totalMetric.dec(dec);
        if (totalMetric.count() < 0) {
            totalMetric.inc(dec);
            logger.warn(
                "Ignoring the operation to deduct memory: {} from RequestStats memory_size metric as it will "
                    + "go negative. Current memory: {}. This is a bug.",
                dec,
                totalMetric.count()
            );
        }
    }

    // Old functions which increment size by passing in an Accountable. Functional but no longer used.
    public void onCached(Accountable key, BytesReference value) {
        totalMetric.inc(key.ramBytesUsed() + value.ramBytesUsed());
    }

    public void onRemoval(Accountable key, BytesReference value, boolean evicted) {
        onRemoval(key.ramBytesUsed(), value, evicted);
    }
}
