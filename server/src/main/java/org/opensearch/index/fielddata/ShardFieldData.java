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
import org.opensearch.common.FieldStats;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.core.common.util.CollectionUtils;
import org.opensearch.core.index.shard.ShardId;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

import static org.opensearch.index.fielddata.FieldDataStats.IS_MEMORY;
import static org.opensearch.index.fielddata.FieldDataStats.ORDERED_STAT_NAMES;
import static org.opensearch.index.fielddata.FieldDataStats.READABLE_KEYS;

/**
 * On heap field data for shards
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class ShardFieldData implements IndexFieldDataCache.Listener {

    private final CounterMetric evictionsMetric = new CounterMetric();
    private final CounterMetric totalMetric = new CounterMetric();
    private final ConcurrentMap<String, MutableFieldStats> perFieldStats = ConcurrentCollections.newConcurrentMap();
    private final CounterMetric countMetric = new CounterMetric();

    public FieldDataStats stats(String... fields) {
        FieldStats fieldStats = null;

        if (CollectionUtils.isEmpty(fields) == false) {
            Map<String, Map<String, Long>> data = new HashMap<>();
            for (Map.Entry<String, MutableFieldStats> entry : perFieldStats.entrySet()) {
                if (Regex.simpleMatch(fields, entry.getKey())) {
                    Map<String, Long> perFieldData = new HashMap<>();
                    perFieldData.put(FieldDataStats.MEMORY_SIZE_IN_BYTES, entry.getValue().getMemorySize());
                    perFieldData.put(FieldDataStats.ITEM_COUNT, entry.getValue().getItems());
                    data.put(entry.getKey(), perFieldData);
                }
            }
            fieldStats = new FieldStats(ORDERED_STAT_NAMES, IS_MEMORY, READABLE_KEYS, data);
        }
        return new FieldDataStats(totalMetric.count(), evictionsMetric.count(), countMetric.count(), fieldStats);
    }

    @Override
    public void onCache(ShardId shardId, String fieldName, Accountable ramUsage) {
        totalMetric.inc(ramUsage.ramBytesUsed());
        countMetric.inc();
        long size = ramUsage.ramBytesUsed();
        MutableFieldStats mutableFieldStats = perFieldStats.get(fieldName);
        if (mutableFieldStats != null) {
            mutableFieldStats.increment(size);
        } else {
            mutableFieldStats = new MutableFieldStats();
            mutableFieldStats.increment(size);
            MutableFieldStats prev = perFieldStats.putIfAbsent(fieldName, mutableFieldStats);
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
        MutableFieldStats mutableFieldStats = perFieldStats.get(fieldName);
        if (sizeInBytes != -1) {
            totalMetric.dec(sizeInBytes);
            if (mutableFieldStats != null) {
                mutableFieldStats.memorySizeMetric.dec(sizeInBytes);
            }
        }
        countMetric.dec();
        if (mutableFieldStats != null) {
            mutableFieldStats.itemsMetric.dec();
        }
    }

    /**
     * Memory + item stats counters for one field.
     */
    class MutableFieldStats {
        final CounterMetric memorySizeMetric;
        final CounterMetric itemsMetric;

        MutableFieldStats() {
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
