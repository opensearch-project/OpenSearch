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

package org.opensearch.index.shard;

import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.metrics.MeanMetric;
import org.opensearch.index.engine.Engine;

import java.util.concurrent.TimeUnit;

/**
 * Internal class that maintains relevant indexing statistics / metrics.
 * @see IndexShard
 *
 * @opensearch.internal
 */
final class InternalIndexingStats implements IndexingOperationListener {
    private final StatsHolder totalStats = new StatsHolder();

    /**
     * Returns the stats, including type specific stats. If the types are null/0 length, then nothing
     * is returned for them. If they are set, then only types provided will be returned, or
     * {@code _all} for all types.
     */
    IndexingStats stats(boolean isThrottled, long currentThrottleInMillis) {
        IndexingStats.Stats total = totalStats.stats(isThrottled, currentThrottleInMillis);
        return new IndexingStats(total);
    }

    @Override
    public Engine.Index preIndex(ShardId shardId, Engine.Index operation) {
        if (operation.origin().isRecovery() == false) {
            totalStats.indexCurrent.inc();
        }
        return operation;
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {
        switch (result.getResultType()) {
            case SUCCESS:
                if (index.origin().isRecovery() == false) {
                    long took = result.getTook();
                    totalStats.indexMetric.inc(took);
                    totalStats.indexCurrent.dec();
                }
                break;
            case FAILURE:
                postIndex(shardId, index, result.getFailure());
                break;
            default:
                throw new IllegalArgumentException("unknown result type: " + result.getResultType());
        }
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Exception ex) {
        if (!index.origin().isRecovery()) {
            totalStats.indexCurrent.dec();
            totalStats.indexFailed.inc();
        }
    }

    @Override
    public Engine.Delete preDelete(ShardId shardId, Engine.Delete delete) {
        if (!delete.origin().isRecovery()) {
            totalStats.deleteCurrent.inc();
        }
        return delete;

    }

    @Override
    public void postDelete(ShardId shardId, Engine.Delete delete, Engine.DeleteResult result) {
        switch (result.getResultType()) {
            case SUCCESS:
                if (!delete.origin().isRecovery()) {
                    long took = result.getTook();
                    totalStats.deleteMetric.inc(took);
                    totalStats.deleteCurrent.dec();
                }
                break;
            case FAILURE:
                postDelete(shardId, delete, result.getFailure());
                break;
            default:
                throw new IllegalArgumentException("unknown result type: " + result.getResultType());
        }
    }

    @Override
    public void postDelete(ShardId shardId, Engine.Delete delete, Exception ex) {
        if (!delete.origin().isRecovery()) {
            totalStats.deleteCurrent.dec();
        }
    }

    void noopUpdate() {
        totalStats.noopUpdates.inc();
    }

    /**
     * Holder for base indexing statistics
     *
     * @opensearch.internal
     */
    static class StatsHolder {
        private final MeanMetric indexMetric = new MeanMetric();
        private final MeanMetric deleteMetric = new MeanMetric();
        private final CounterMetric indexCurrent = new CounterMetric();
        private final CounterMetric indexFailed = new CounterMetric();
        private final CounterMetric deleteCurrent = new CounterMetric();
        private final CounterMetric noopUpdates = new CounterMetric();

        IndexingStats.Stats stats(boolean isThrottled, long currentThrottleMillis) {
            return new IndexingStats.Stats(
                indexMetric.count(),
                TimeUnit.NANOSECONDS.toMillis(indexMetric.sum()),
                indexCurrent.count(),
                indexFailed.count(),
                deleteMetric.count(),
                TimeUnit.NANOSECONDS.toMillis(deleteMetric.sum()),
                deleteCurrent.count(),
                noopUpdates.count(),
                isThrottled,
                TimeUnit.MILLISECONDS.toMillis(currentThrottleMillis)
            );
        }
    }
}
