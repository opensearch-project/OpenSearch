/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.IndexWriter.IndexReaderWarmer;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentReader;
import org.opensearch.Version;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.logging.Loggers;
import org.opensearch.index.merge.MergedSegmentTransferTracker;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Implementation of a {@link IndexReaderWarmer} for merged segment replication in
 * local on-disk and remote store enabled domains.
 *
 * @opensearch.internal
 */
public class MergedSegmentWarmer implements IndexReaderWarmer {
    private final TransportService transportService;
    private final RecoverySettings recoverySettings;
    private final ClusterService clusterService;
    private final IndexShard indexShard;
    private final MergedSegmentTransferTracker mergedSegmentTransferTracker;
    private final Logger logger;

    public MergedSegmentWarmer(
        TransportService transportService,
        RecoverySettings recoverySettings,
        ClusterService clusterService,
        IndexShard indexShard
    ) {
        this.transportService = transportService;
        this.recoverySettings = recoverySettings;
        this.clusterService = clusterService;
        this.indexShard = indexShard;
        this.mergedSegmentTransferTracker = indexShard.mergedSegmentTransferTracker();
        this.logger = Loggers.getLogger(getClass(), indexShard.shardId());
    }

    @Override
    public void warm(LeafReader leafReader) throws IOException {

        long startTime = System.currentTimeMillis();
        long elapsedTime = -1;
        boolean shouldWarm = false;
        try {
            SegmentCommitInfo segmentCommitInfo = segmentCommitInfo(leafReader);
            // If shouldWarm fails, we increment the warmFailureCount
            // However, the time taken by shouldWarm is not accounted for in the totalWarmTime
            shouldWarm = shouldWarm(segmentCommitInfo);
            if (shouldWarm == false) {
                return;
            }
            mergedSegmentTransferTracker.incrementTotalWarmInvocationsCount();
            mergedSegmentTransferTracker.incrementOngoingWarms();
            logger.trace(() -> new ParameterizedMessage("Warming segment: {}", segmentCommitInfo));
            indexShard.publishMergedSegment(segmentCommitInfo);
            elapsedTime = System.currentTimeMillis() - startTime;
            long finalElapsedTime = elapsedTime;
            logger.trace(() -> {
                long segmentSize = -1;
                try {
                    segmentSize = segmentCommitInfo.sizeInBytes();
                } catch (IOException ignored) {}
                return new ParameterizedMessage(
                    "Completed segment warming for {}. Size: {}B, Timing: {}ms",
                    segmentCommitInfo.info.name,
                    segmentSize,
                    finalElapsedTime
                );
            });
        } catch (Throwable t) {
            logger.warn(() -> new ParameterizedMessage("Failed to warm segment. Continuing. {}", segmentCommitInfo(leafReader)), t);
            mergedSegmentTransferTracker.incrementTotalWarmFailureCount();
        } finally {
            if (shouldWarm == true) {
                if (elapsedTime == -1) {
                    elapsedTime = System.currentTimeMillis() - startTime;
                }
                mergedSegmentTransferTracker.addTotalWarmTimeMillis(elapsedTime);
                mergedSegmentTransferTracker.decrementOngoingWarms();
            }
        }
    }

    // package-private for tests
    SegmentCommitInfo segmentCommitInfo(LeafReader leafReader) {

        // IndexWriter.IndexReaderWarmer#warm is called by IndexWriter#mergeMiddle. The type of leafReader should be SegmentReader.
        assert leafReader instanceof SegmentReader;
        assert indexShard.indexSettings().isSegRepLocalEnabled() || indexShard.indexSettings().isRemoteStoreEnabled();
        try {
            return ((SegmentReader) leafReader).getSegmentInfo();
        } catch (Throwable e) {
            logger.warn("Unable to get segment info from leafReader. Continuing.", e);
        }
        return null;
    }

    // package-private for tests
    boolean shouldWarm(SegmentCommitInfo segmentCommitInfo) throws IOException {
        // Min node version check ensures that we only warm, when all nodes expect it
        Version minNodeVersion = clusterService.state().nodes().getMinNodeVersion();
        if (Version.V_3_4_0.compareTo(minNodeVersion) > 0) {
            return false;
        }

        if (indexShard.getRecoverySettings().isMergedSegmentReplicationWarmerEnabled() == false) {
            return false;
        }

        // in case we are unable to gauge the size of the merged segment segmentCommitInfo.sizeInBytes throws IOException
        // we would not warm the segment
        if (segmentCommitInfo.info == null || segmentCommitInfo.info.dir == null) {
            return false;
        }

        long segmentSize = segmentCommitInfo.sizeInBytes();
        double threshold = indexShard.getRecoverySettings().getMergedSegmentWarmerMinSegmentSizeThreshold().getBytes();
        if (segmentSize < threshold) {
            logger.trace(
                () -> new ParameterizedMessage(
                    "Skipping warm for segment {}. SegmentSize {}B is less than the configured threshold {}B.",
                    segmentCommitInfo.info.name,
                    segmentSize,
                    threshold
                )
            );
            return false;
        }

        return true;
    }
}
