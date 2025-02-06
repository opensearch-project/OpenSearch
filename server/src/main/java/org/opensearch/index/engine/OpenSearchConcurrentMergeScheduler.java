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

package org.opensearch.index.engine;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.ConcurrentMergeScheduler;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeScheduler;
import org.opensearch.common.logging.Loggers;
import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.metrics.MeanMetric;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.ConcurrentCollections;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.MergeSchedulerConfig;
import org.opensearch.index.merge.MergeStats;
import org.opensearch.index.merge.OnGoingMerge;

import java.io.IOException;
import java.util.Collections;
import java.util.Locale;
import java.util.Set;

/**
 * An extension to the {@link ConcurrentMergeScheduler} that provides tracking on merge times, total
 * and current merges.
 *
 * @opensearch.internal
 */
class OpenSearchConcurrentMergeScheduler extends ConcurrentMergeScheduler {

    protected final Logger logger;
    private final Settings indexSettings;
    private final ShardId shardId;

    private final MeanMetric totalMerges = new MeanMetric();
    private final CounterMetric totalMergesNumDocs = new CounterMetric();
    private final CounterMetric totalMergesSizeInBytes = new CounterMetric();
    private final CounterMetric currentMerges = new CounterMetric();
    private final CounterMetric currentMergesNumDocs = new CounterMetric();
    private final CounterMetric currentMergesSizeInBytes = new CounterMetric();
    private final CounterMetric totalMergeStoppedTime = new CounterMetric();
    private final CounterMetric totalMergeThrottledTime = new CounterMetric();

    private final Set<OnGoingMerge> onGoingMerges = ConcurrentCollections.newConcurrentSet();
    private final Set<OnGoingMerge> readOnlyOnGoingMerges = Collections.unmodifiableSet(onGoingMerges);
    private final MergeSchedulerConfig config;

    OpenSearchConcurrentMergeScheduler(ShardId shardId, IndexSettings indexSettings) {
        this.config = indexSettings.getMergeSchedulerConfig();
        this.shardId = shardId;
        this.indexSettings = indexSettings.getSettings();
        this.logger = Loggers.getLogger(getClass(), shardId);
        refreshConfig();
    }

    public Set<OnGoingMerge> onGoingMerges() {
        return readOnlyOnGoingMerges;
    }

    /**
     *Refactored to move the getSegmentName from OneMergeHelper which used to be part of :server org.apache.lucene.index
     */
    private static String getSegmentName(MergePolicy.OneMerge merge) {
        return merge.getMergeInfo() != null ? merge.getMergeInfo().info.name : "_na_";
    }

    @Override
    protected void doMerge(MergeSource mergeSource, MergePolicy.OneMerge merge) throws IOException {
        int totalNumDocs = merge.totalNumDocs();
        long totalSizeInBytes = merge.totalBytesSize();
        long timeNS = System.nanoTime();
        currentMerges.inc();
        currentMergesNumDocs.inc(totalNumDocs);
        currentMergesSizeInBytes.inc(totalSizeInBytes);

        OnGoingMerge onGoingMerge = new OnGoingMerge(merge);
        onGoingMerges.add(onGoingMerge);

        if (logger.isTraceEnabled()) {
            logger.trace(
                "merge [{}] starting..., merging [{}] segments, [{}] docs, [{}] size, into [{}] estimated_size",
                getSegmentName(merge),
                merge.segments.size(),
                totalNumDocs,
                new ByteSizeValue(totalSizeInBytes),
                new ByteSizeValue(merge.estimatedMergeBytes)
            );
        }
        try {
            beforeMerge(onGoingMerge);
            super.doMerge(mergeSource, merge);
        } finally {
            long tookMS = TimeValue.nsecToMSec(System.nanoTime() - timeNS);

            onGoingMerges.remove(onGoingMerge);
            afterMerge(onGoingMerge);

            currentMerges.dec();
            currentMergesNumDocs.dec(totalNumDocs);
            currentMergesSizeInBytes.dec(totalSizeInBytes);

            totalMergesNumDocs.inc(totalNumDocs);
            totalMergesSizeInBytes.inc(totalSizeInBytes);
            totalMerges.inc(tookMS);
            long stoppedMS = TimeValue.nsecToMSec(
                merge.getMergeProgress().getPauseTimes().get(MergePolicy.OneMergeProgress.PauseReason.STOPPED)
            );
            long throttledMS = TimeValue.nsecToMSec(
                merge.getMergeProgress().getPauseTimes().get(MergePolicy.OneMergeProgress.PauseReason.PAUSED)
            );
            totalMergeStoppedTime.inc(stoppedMS);
            totalMergeThrottledTime.inc(throttledMS);

            String message = String.format(
                Locale.ROOT,
                "merge segment [%s] done: took [%s], [%,.1f MB], [%,d docs], [%s stopped], " + "[%s throttled]",
                getSegmentName(merge),
                TimeValue.timeValueMillis(tookMS),
                totalSizeInBytes / 1024f / 1024f,
                totalNumDocs,
                TimeValue.timeValueMillis(stoppedMS),
                TimeValue.timeValueMillis(throttledMS)
            );

            if (tookMS > 20000) { // if more than 20 seconds, DEBUG log it
                logger.debug("{}", message);
            } else if (logger.isTraceEnabled()) {
                logger.trace("{}", message);
            }
        }
    }

    /**
     * A callback allowing for custom logic before an actual merge starts.
     */
    protected void beforeMerge(OnGoingMerge merge) {}

    /**
     * A callback allowing for custom logic before an actual merge starts.
     */
    protected void afterMerge(OnGoingMerge merge) {}

    @Override
    public MergeScheduler clone() {
        // Lucene IW makes a clone internally but since we hold on to this instance
        // the clone will just be the identity.
        return this;
    }

    @Override
    protected boolean maybeStall(MergeSource mergeSource) {
        // Don't stall here, because we do our own index throttling (in InternalEngine.IndexThrottle) when merges can't keep up
        return true;
    }

    @Override
    protected MergeThread getMergeThread(MergeSource mergeSource, MergePolicy.OneMerge merge) throws IOException {
        MergeThread thread = super.getMergeThread(mergeSource, merge);
        thread.setName(
            OpenSearchExecutors.threadName(indexSettings, "[" + shardId.getIndexName() + "][" + shardId.id() + "]: " + thread.getName())
        );
        return thread;
    }

    MergeStats stats() {
        final MergeStats mergeStats = new MergeStats();
        mergeStats.add(
            totalMerges.count(),
            totalMerges.sum(),
            totalMergesNumDocs.count(),
            totalMergesSizeInBytes.count(),
            currentMerges.count(),
            currentMergesNumDocs.count(),
            currentMergesSizeInBytes.count(),
            totalMergeStoppedTime.count(),
            totalMergeThrottledTime.count(),
            config.isAutoThrottle() ? getIORateLimitMBPerSec() : Double.POSITIVE_INFINITY
        );
        return mergeStats;
    }

    void refreshConfig() {
        if (this.getMaxMergeCount() != config.getMaxMergeCount() || this.getMaxThreadCount() != config.getMaxThreadCount()) {
            this.setMaxMergesAndThreads(config.getMaxMergeCount(), config.getMaxThreadCount());
        }
        boolean isEnabled = getIORateLimitMBPerSec() != Double.POSITIVE_INFINITY;
        if (config.isAutoThrottle() && isEnabled == false) {
            enableAutoIOThrottle();
        } else if (config.isAutoThrottle() == false && isEnabled) {
            disableAutoIOThrottle();
        }
    }

}
