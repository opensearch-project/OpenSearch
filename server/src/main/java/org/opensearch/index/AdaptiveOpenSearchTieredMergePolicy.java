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

import org.apache.lucene.index.FilterMergePolicy;
import org.apache.lucene.index.MergePolicy;
import org.apache.lucene.index.MergeTrigger;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.index.TieredMergePolicy;

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Adaptive variant of {@link OpenSearchTieredMergePolicy} that updates merge policy parameters
 * at merge time based on current segment topology (derived from SegmentInfos). This avoids the
 * need to inject a Store to estimate shard size up-front.
 *
 * Strategy:
 * - On each {@link #findMerges(MergeTrigger, SegmentInfos, MergePolicy.MergeContext)} call, compute the
 *   current total shard size from SegmentInfos and adjust:
 *   - max merged segment (caps at 5GB)
 *   - floor segment size
 *   - segments per tier
 * - Forced merges continue to respect the OpenSearch behavior (unlimited max for forced merge).
 *
 * @opensearch.internal
 */
final class AdaptiveOpenSearchTieredMergePolicy extends FilterMergePolicy {

    final TieredMergePolicy regularMergePolicy;
    final TieredMergePolicy forcedMergePolicy;
    private final ReentrantReadWriteLock settingsLock;

    AdaptiveOpenSearchTieredMergePolicy() {
        this(null);
    }

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
        for (SegmentCommitInfo info : infos) {
            try {
                totalSizeBytes += info.sizeInBytes();
            } catch (IOException e) {
                // best-effort: skip segments we cannot size
            }
        }

        // Apply smooth interpolation-based settings
        double maxMergedSegmentMB = bytesToMB(calculateSmoothMaxSegmentSize(totalSizeBytes));
        double floorSegmentMB = bytesToMB(calculateSmoothFloorSegmentSize(totalSizeBytes));
        double segmentsPerTier = calculateSmoothSegmentsPerTier(totalSizeBytes);

        // Synchronize settings updates to prevent race conditions with concurrent setter calls
        if (settingsLock != null) {
            settingsLock.writeLock().lock();
            try {
                regularMergePolicy.setMaxMergedSegmentMB(maxMergedSegmentMB);
                regularMergePolicy.setFloorSegmentMB(floorSegmentMB);
                regularMergePolicy.setSegmentsPerTier(segmentsPerTier);
            } finally {
                settingsLock.writeLock().unlock();
            }
        } else {
            // Fallback for cases where lock is not provided (shouldn't happen in production)
            regularMergePolicy.setMaxMergedSegmentMB(maxMergedSegmentMB);
            regularMergePolicy.setFloorSegmentMB(floorSegmentMB);
            regularMergePolicy.setSegmentsPerTier(segmentsPerTier);
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
        if (settingsLock != null) {
            settingsLock.writeLock().lock();
            try {
                regularMergePolicy.setForceMergeDeletesPctAllowed(forceMergeDeletesPctAllowed);
                forcedMergePolicy.setForceMergeDeletesPctAllowed(forceMergeDeletesPctAllowed);
            } finally {
                settingsLock.writeLock().unlock();
            }
        } else {
            regularMergePolicy.setForceMergeDeletesPctAllowed(forceMergeDeletesPctAllowed);
            forcedMergePolicy.setForceMergeDeletesPctAllowed(forceMergeDeletesPctAllowed);
        }
    }

    public double getForceMergeDeletesPctAllowed() {
        return regularMergePolicy.getForceMergeDeletesPctAllowed();
    }

    public void setFloorSegmentMB(double mbFrac) {
        if (settingsLock != null) {
            settingsLock.writeLock().lock();
            try {
                regularMergePolicy.setFloorSegmentMB(mbFrac);
                forcedMergePolicy.setFloorSegmentMB(mbFrac);
            } finally {
                settingsLock.writeLock().unlock();
            }
        } else {
            regularMergePolicy.setFloorSegmentMB(mbFrac);
            forcedMergePolicy.setFloorSegmentMB(mbFrac);
        }
    }

    public double getFloorSegmentMB() {
        return regularMergePolicy.getFloorSegmentMB();
    }

    public void setMaxMergeAtOnce(int maxMergeAtOnce) {
        if (settingsLock != null) {
            settingsLock.writeLock().lock();
            try {
                regularMergePolicy.setMaxMergeAtOnce(maxMergeAtOnce);
                forcedMergePolicy.setMaxMergeAtOnce(maxMergeAtOnce);
            } finally {
                settingsLock.writeLock().unlock();
            }
        } else {
            regularMergePolicy.setMaxMergeAtOnce(maxMergeAtOnce);
            forcedMergePolicy.setMaxMergeAtOnce(maxMergeAtOnce);
        }
    }

    public int getMaxMergeAtOnce() {
        return regularMergePolicy.getMaxMergeAtOnce();
    }

    // only setter that must NOT delegate to the forced merge policy
    public void setMaxMergedSegmentMB(double mbFrac) {
        if (settingsLock != null) {
            settingsLock.writeLock().lock();
            try {
                regularMergePolicy.setMaxMergedSegmentMB(mbFrac);
            } finally {
                settingsLock.writeLock().unlock();
            }
        } else {
            regularMergePolicy.setMaxMergedSegmentMB(mbFrac);
        }
    }

    public double getMaxMergedSegmentMB() {
        return regularMergePolicy.getMaxMergedSegmentMB();
    }

    public void setSegmentsPerTier(double segmentsPerTier) {
        if (settingsLock != null) {
            settingsLock.writeLock().lock();
            try {
                regularMergePolicy.setSegmentsPerTier(segmentsPerTier);
                forcedMergePolicy.setSegmentsPerTier(segmentsPerTier);
            } finally {
                settingsLock.writeLock().unlock();
            }
        } else {
            regularMergePolicy.setSegmentsPerTier(segmentsPerTier);
            forcedMergePolicy.setSegmentsPerTier(segmentsPerTier);
        }
    }

    public double getSegmentsPerTier() {
        return regularMergePolicy.getSegmentsPerTier();
    }

    public void setDeletesPctAllowed(double deletesPctAllowed) {
        if (settingsLock != null) {
            settingsLock.writeLock().lock();
            try {
                regularMergePolicy.setDeletesPctAllowed(deletesPctAllowed);
                forcedMergePolicy.setDeletesPctAllowed(deletesPctAllowed);
            } finally {
                settingsLock.writeLock().unlock();
            }
        } else {
            regularMergePolicy.setDeletesPctAllowed(deletesPctAllowed);
            forcedMergePolicy.setDeletesPctAllowed(deletesPctAllowed);
        }
    }

    public double getDeletesPctAllowed() {
        return regularMergePolicy.getDeletesPctAllowed();
    }

    private static double bytesToMB(long bytes) {
        return (double) bytes / (1024.0 * 1024.0);
    }

    private static long calculateSmoothMaxSegmentSize(long shardSizeBytes) {
        double logSize = Math.log10(Math.max(1L, shardSizeBytes));
        // Thresholds are based on decimal powers: log10 < 8.0 ≈ < 100MB, < 9.0 ≈ < 1GB, etc.
        // Returned sizes use binary units (MiB/GiB via 1024²)
        if (logSize < 8.0) { // < 10^8 bytes (≈ 95.4 MiB)
            return 50L * 1024 * 1024; // 50 MiB
        } else if (logSize < 9.0) { // 10^8 - 10^9 bytes (≈ 95.4 MiB - 953.7 MiB)
            double ratio = (logSize - 8.0) / 1.0;
            long a = 50L * 1024 * 1024;
            long b = 200L * 1024 * 1024;
            return (long) (a + ratio * (b - a));
        } else if (logSize < 10.0) { // 10^9 - 10^10 bytes (≈ 953.7 MiB - 9.31 GiB)
            double ratio = (logSize - 9.0) / 1.0;
            long a = 200L * 1024 * 1024;
            long b = 1L * 1024 * 1024 * 1024;
            return (long) (a + ratio * (b - a));
        } else if (logSize < 11.0) { // 10^10 - 10^11 bytes (≈ 9.31 GiB - 93.1 GiB)
            double ratio = (logSize - 10.0) / 1.0;
            long a = 1L * 1024 * 1024 * 1024;
            long b = 5L * 1024 * 1024 * 1024; // cap at 5 GiB
            return (long) (a + ratio * (b - a));
        } else { // >= 10^11 bytes (≈ 93.1 GiB)
            return 5L * 1024 * 1024 * 1024; // 5 GiB
        }
    }

    private static long calculateSmoothFloorSegmentSize(long shardSizeBytes) {
        double logSize = Math.log10(Math.max(1L, shardSizeBytes));
        // Thresholds are based on decimal powers: log10 < 8.0 ≈ < 100MB, < 9.0 ≈ < 1GB, etc.
        // Returned sizes use binary units (MiB/GiB via 1024²)
        if (logSize < 8.0) { // < 10^8 bytes (≈ 95.4 MiB)
            return 10L * 1024 * 1024; // 10 MiB
        } else if (logSize < 9.0) { // 10^8 - 10^9 bytes (≈ 95.4 MiB - 953.7 MiB)
            double ratio = (logSize - 8.0) / 1.0;
            long a = 10L * 1024 * 1024;
            long b = 25L * 1024 * 1024;
            return (long) (a + ratio * (b - a));
        } else if (logSize < 10.0) { // 10^9 - 10^10 bytes (≈ 953.7 MiB - 9.31 GiB)
            double ratio = (logSize - 9.0) / 1.0;
            long a = 25L * 1024 * 1024;
            long b = 50L * 1024 * 1024;
            return (long) (a + ratio * (b - a));
        } else if (logSize < 11.0) { // 10^10 - 10^11 bytes (≈ 9.31 GiB - 93.1 GiB)
            double ratio = (logSize - 10.0) / 1.0;
            long a = 50L * 1024 * 1024;
            long b = 100L * 1024 * 1024;
            return (long) (a + ratio * (b - a));
        } else { // >= 10^11 bytes (≈ 93.1 GiB)
            return 100L * 1024 * 1024; // 100 MiB
        }
    }

    private static double calculateSmoothSegmentsPerTier(long shardSizeBytes) {
        double logSize = Math.log10(Math.max(1L, shardSizeBytes));
        // Thresholds are based on decimal powers: log10 < 8.0 ≈ < 100MB, < 9.0 ≈ < 1GB, etc.
        if (logSize < 8.0) { // < 10^8 bytes (≈ 95.4 MiB)
            return 5.0;
        } else if (logSize < 9.0) { // 10^8 - 10^9 bytes (≈ 95.4 MiB - 953.7 MiB)
            double ratio = (logSize - 8.0) / 1.0;
            return 5.0 + ratio * (8.0 - 5.0);
        } else if (logSize < 10.0) { // 10^9 - 10^10 bytes (≈ 953.7 MiB - 9.31 GiB)
            double ratio = (logSize - 9.0) / 1.0;
            return 8.0 + ratio * (10.0 - 8.0);
        } else if (logSize < 11.0) { // 10^10 - 10^11 bytes (≈ 9.31 GiB - 93.1 GiB)
            double ratio = (logSize - 10.0) / 1.0;
            return 10.0 + ratio * (12.0 - 10.0);
        } else { // >= 10^11 bytes (≈ 93.1 GiB)
            return 12.0;
        }
    }
}
