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

package org.opensearch.index.fielddata;

import org.apache.lucene.util.Accountable;
import org.opensearch.common.FieldMemoryStats;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.core.common.util.CollectionUtils;
import org.opensearch.core.index.shard.ShardId;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

/**
 * On heap field data for shards
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ShardFieldData implements IndexFieldDataCache.Listener {

    private final CounterMetric evictionsMetric = new CounterMetric();
    private final CounterMetric totalMetric = new CounterMetric();
    private final ConcurrentMap<String, FieldStats> perFieldStats = ConcurrentCollections.newConcurrentMap();
    private final CounterMetric countMetric = new CounterMetric();

    public FieldDataStats stats(String... fields) {
        Map<String, Long> fieldTotals = null;
        Map<String, Long> fieldCounts = null;
        if (CollectionUtils.isEmpty(fields) == false) {
            fieldTotals = new HashMap<>();
            fieldCounts = new HashMap<>();
            for (Map.Entry<String, FieldStats> entry : perFieldStats.entrySet()) {
                if (Regex.simpleMatch(fields, entry.getKey())) {
                    fieldTotals.put(entry.getKey(), entry.getValue().getMemorySize());
                    fieldCounts.put(entry.getKey(), entry.getValue().getItems());
                }
            }
        }
        return new FieldDataStats(
            totalMetric.count(),
            evictionsMetric.count(),
            fieldTotals == null ? null : new FieldMemoryStats(fieldTotals),
            countMetric.count(),
            fieldCounts == null ? null : new FieldMemoryStats(fieldCounts)
        );
    }

    @Override
    public void onCache(ShardId shardId, String fieldName, Accountable ramUsage) {
        totalMetric.inc(ramUsage.ramBytesUsed());
        countMetric.inc();
        long size = ramUsage.ramBytesUsed();
        FieldStats fieldStats = perFieldStats.get(fieldName);
        if (fieldStats != null) {
            fieldStats.increment(size);
        } else {
            fieldStats = new FieldStats();
            fieldStats.increment(size);
            FieldStats prev = perFieldStats.putIfAbsent(fieldName, fieldStats);
            if (prev != null) {
                prev.increment(size);
            }
        }
    }

    @Override
    public void onRemoval(ShardId shardId, String fieldName, boolean wasEvicted, long sizeInBytes) {
        if (wasEvicted) {
            evictionsMetric.inc();
        }
        FieldStats fieldStats = perFieldStats.get(fieldName);
        if (sizeInBytes != -1) {
            totalMetric.dec(sizeInBytes);
            if (fieldStats != null) {
                fieldStats.memorySizeMetric.dec(sizeInBytes);
            }
        }
        countMetric.dec();
        if (fieldStats != null) {
            fieldStats.itemsMetric.dec();
        }
    }

    /**
     * Memory + item stats counters for one field.
     */
    class FieldStats {
        final CounterMetric memorySizeMetric;
        final CounterMetric itemsMetric;

        FieldStats() {
            this.memorySizeMetric = new CounterMetric();
            this.itemsMetric = new CounterMetric();
        }

        void increment(long sizeInBytes) {
            memorySizeMetric.inc(sizeInBytes);
            itemsMetric.inc();
        }

        long getMemorySize() {
            return memorySizeMetric.count();
        }

        long getItems() {
            return itemsMetric.count();
        }
    }
}
