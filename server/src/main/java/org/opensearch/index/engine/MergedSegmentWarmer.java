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
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentReader;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.logging.Loggers;
import org.opensearch.index.merge.MergedSegmentTransferTracker;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Implementation of a {@link IndexWriter.IndexReaderWarmer} for merged segment replication in
 * local on-disk and remote store enabled domains.
 *
 * @opensearch.internal
 */
public class MergedSegmentWarmer implements IndexWriter.IndexReaderWarmer {
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
        mergedSegmentTransferTracker.incrementTotalWarmInvocationsCount();
        mergedSegmentTransferTracker.incrementOngoingWarms();
        // IndexWriter.IndexReaderWarmer#warm is called by IndexWriter#mergeMiddle. The type of leafReader should be SegmentReader.
        assert leafReader instanceof SegmentReader;
        assert indexShard.indexSettings().isSegRepLocalEnabled() || indexShard.indexSettings().isRemoteStoreEnabled();
        long startTime = System.currentTimeMillis();
        long elapsedTime = 0;
        try {
            SegmentCommitInfo segmentCommitInfo = ((SegmentReader) leafReader).getSegmentInfo();
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
        } catch (IOException e) {
            mergedSegmentTransferTracker.incrementTotalWarmFailureCount();
        } finally {
            mergedSegmentTransferTracker.addTotalWarmTimeMillis(elapsedTime);
            mergedSegmentTransferTracker.decrementOngoingWarms();
        }
    }

    // package-private for tests
    boolean shouldWarm() {
        return indexShard.getRecoverySettings().isMergedSegmentReplicationWarmerEnabled() == true;
    }
}
