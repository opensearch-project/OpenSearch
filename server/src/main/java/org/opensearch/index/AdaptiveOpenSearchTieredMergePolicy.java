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

package org.opensearch.index;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.FilterMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.TieredMergePolicy;

import java.io.IOException;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Adaptive merge policy implementation similar to
 * {@link OpenSearchTieredMergePolicy} that updates
 * merge policy parameters at merge time based on current segment topology
 * (derived from SegmentInfos).
 * This avoids the need to inject a Store to estimate shard size up-front.
 *
 * Strategy:
 * - On each
 * {@link #findMerges(MergeTrigger, SegmentInfos, MergePolicy.MergeContext)}
 * call, compute the
 * current total shard size from SegmentInfos and adjust:
 * - max merged segment (caps at 5 GiB)
 * - floor segment size
 * - segments per tier
 * - Forced merges continue to respect the OpenSearch behavior (unlimited max
 * for forced merge).
 *
 * @opensearch.internal
 */
final class AdaptiveOpenSearchTieredMergePolicy extends FilterMergePolicy {

    private static final Logger logger = LogManager.getLogger(AdaptiveOpenSearchTieredMergePolicy.class);

    final TieredMergePolicy regularMergePolicy;
    final TieredMergePolicy forcedMergePolicy;
    private final ReentrantReadWriteLock settingsLock;

    AdaptiveOpenSearchTieredMergePolicy(ReentrantReadWriteLock settingsLock) {
        super(new TieredMergePolicy());
        regularMergePolicy = (TieredMergePolicy) in;
        forcedMergePolicy = new TieredMergePolicy();
        forcedMergePolicy.setMaxMergedSegmentMB(Double.POSITIVE_INFINITY); // unlimited for forced merges
        this.settingsLock = settingsLock;
    }

    @Override
    public MergeSpecification findMerges(MergeTrigger mergeTrigger, SegmentInfos infos, MergeContext mergeContext) throws IOException {
        // Recompute adaptive settings from the current topology before planning merges
        long totalSizeBytes = 0L;
        int failedSegments = 0;
        int totalSegments = infos.size();

        for (SegmentCommitInfo info : infos) {
            try {
                totalSizeBytes += info.sizeInBytes();
            } catch (IOException e) {
                // Best-effort: skip segments we cannot size. This can happen if segment files
                // are temporarily unavailable (e.g., during remote store replication).
                // If many segments fail, the shard size will be underestimated, leading to
                // smaller adaptive settings than optimal. Fallback: if totalSizeBytes is 0,
                // the calculation methods will clamp to 1 byte, producing the smallest
                // settings.
                failedSegments++;
                if (logger.isDebugEnabled()) {
                    logger.debug(
                        () -> new ParameterizedMessage(
                            "Failed to get size for segment [{}], skipping from shard size calculation",
                            info.info.name
                        ),
                        e
                    );
                }
            }
        }

        // Log warning if significant number of segments failed or all segments failed
        if (failedSegments > 0) {
            double failureRate = (double) failedSegments / totalSegments;
            if (totalSizeBytes == 0L) {
                // All segments failed - this will produce minimum adaptive settings
                logger.warn(
                    "All {} segments failed to report size during adaptive merge policy calculation. "
                        + "Using minimum adaptive settings (50 MiB max segment, 10 MiB floor, 5 segments per tier). "
                        + "This may result in suboptimal merge behavior.",
                    totalSegments
                );
            } else if (failureRate > 0.5) {
                // More than half failed - significant underestimation likely
                logger.warn(
                    "{} of {} segments ({}%) failed to report size during adaptive merge policy calculation. "
                        + "Shard size may be significantly underestimated, leading to smaller adaptive settings than optimal.",
                    failedSegments,
                    totalSegments,
                    String.format(Locale.ROOT, "%.1f", failureRate * 100.0)
                );
            } else if (logger.isTraceEnabled()) {
                // Log at trace level for minor failures
                logger.trace(
                    "{} of {} segments failed to report size (shard size may be slightly underestimated)",
                    failedSegments,
                    totalSegments
                );
            }
        }

        // Apply smooth interpolation-based settings
        double maxMergedSegmentMB = bytesToMB(AdaptiveMergePolicyCalculator.calculateSmoothMaxSegmentSize(totalSizeBytes));
        double floorSegmentMB = bytesToMB(AdaptiveMergePolicyCalculator.calculateSmoothFloorSegmentSize(totalSizeBytes));
        double segmentsPerTier = AdaptiveMergePolicyCalculator.calculateSmoothSegmentsPerTier(totalSizeBytes);

        // Synchronize settings updates to prevent race conditions with concurrent
        // setter calls
        settingsLock.writeLock().lock();
        try {
            regularMergePolicy.setMaxMergedSegmentMB(maxMergedSegmentMB);
            regularMergePolicy.setFloorSegmentMB(floorSegmentMB);
            regularMergePolicy.setSegmentsPerTier(segmentsPerTier);
        } finally {
            settingsLock.writeLock().unlock();
        }
        // Keep other defaults as configured externally (deletes %, etc.)

        return regularMergePolicy.findMerges(mergeTrigger, infos, mergeContext);
    }

    @Override
    public MergeSpecification findForcedMerges(
        SegmentInfos infos,
        int maxSegmentCount,
        Map<SegmentCommitInfo, Boolean> segmentsToMerge,
        MergeContext mergeContext
    ) throws IOException {
        return forcedMergePolicy.findForcedMerges(infos, maxSegmentCount, segmentsToMerge, mergeContext);
    }

    @Override
    public MergeSpecification findForcedDeletesMerges(SegmentInfos infos, MergeContext mergeContext) throws IOException {
        return regularMergePolicy.findForcedDeletesMerges(infos, mergeContext);
    }

    public void setForceMergeDeletesPctAllowed(double forceMergeDeletesPctAllowed) {
        settingsLock.writeLock().lock();
        try {
            regularMergePolicy.setForceMergeDeletesPctAllowed(forceMergeDeletesPctAllowed);
            forcedMergePolicy.setForceMergeDeletesPctAllowed(forceMergeDeletesPctAllowed);
        } finally {
            settingsLock.writeLock().unlock();
        }
    }

    public double getForceMergeDeletesPctAllowed() {
        settingsLock.readLock().lock();
        try {
            return regularMergePolicy.getForceMergeDeletesPctAllowed();
        } finally {
            settingsLock.readLock().unlock();
        }
    }

    public void setFloorSegmentMB(double mbFrac) {
        settingsLock.writeLock().lock();
        try {
            regularMergePolicy.setFloorSegmentMB(mbFrac);
            forcedMergePolicy.setFloorSegmentMB(mbFrac);
        } finally {
            settingsLock.writeLock().unlock();
        }
    }

    public double getFloorSegmentMB() {
        settingsLock.readLock().lock();
        try {
            return regularMergePolicy.getFloorSegmentMB();
        } finally {
            settingsLock.readLock().unlock();
        }
    }

    public void setMaxMergeAtOnce(int maxMergeAtOnce) {
        settingsLock.writeLock().lock();
        try {
            regularMergePolicy.setMaxMergeAtOnce(maxMergeAtOnce);
            forcedMergePolicy.setMaxMergeAtOnce(maxMergeAtOnce);
        } finally {
            settingsLock.writeLock().unlock();
        }
    }

    public int getMaxMergeAtOnce() {
        settingsLock.readLock().lock();
        try {
            return regularMergePolicy.getMaxMergeAtOnce();
        } finally {
            settingsLock.readLock().unlock();
        }
    }

    // only setter that must NOT delegate to the forced merge policy
    public void setMaxMergedSegmentMB(double mbFrac) {
        settingsLock.writeLock().lock();
        try {
            regularMergePolicy.setMaxMergedSegmentMB(mbFrac);
        } finally {
            settingsLock.writeLock().unlock();
        }
    }

    public double getMaxMergedSegmentMB() {
        settingsLock.readLock().lock();
        try {
            return regularMergePolicy.getMaxMergedSegmentMB();
        } finally {
            settingsLock.readLock().unlock();
        }
    }

    public void setSegmentsPerTier(double segmentsPerTier) {
        settingsLock.writeLock().lock();
        try {
            regularMergePolicy.setSegmentsPerTier(segmentsPerTier);
            forcedMergePolicy.setSegmentsPerTier(segmentsPerTier);
        } finally {
            settingsLock.writeLock().unlock();
        }
    }

    public double getSegmentsPerTier() {
        settingsLock.readLock().lock();
        try {
            return regularMergePolicy.getSegmentsPerTier();
        } finally {
            settingsLock.readLock().unlock();
        }
    }

    public void setDeletesPctAllowed(double deletesPctAllowed) {
        settingsLock.writeLock().lock();
        try {
            regularMergePolicy.setDeletesPctAllowed(deletesPctAllowed);
            forcedMergePolicy.setDeletesPctAllowed(deletesPctAllowed);
        } finally {
            settingsLock.writeLock().unlock();
        }
    }

    public double getDeletesPctAllowed() {
        settingsLock.readLock().lock();
        try {
            return regularMergePolicy.getDeletesPctAllowed();
        } finally {
            settingsLock.readLock().unlock();
        }
    }

    private static double bytesToMB(long bytes) {
        return (double) bytes / (1024.0 * 1024.0);
    }

}
