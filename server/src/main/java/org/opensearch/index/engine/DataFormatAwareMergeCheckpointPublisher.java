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
import org.opensearch.Version;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.logging.Loggers;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.merge.MergedSegmentTransferTracker;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.FileMetadata;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.indices.replication.checkpoint.MergedSegmentCheckpoint;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * DFA analog of {@link MergedSegmentWarmer}. Publishes merged segment checkpoints to replicas
 * after a DFA merge completes, with the same guards, metrics and error-handling as the Lucene
 * path. Takes an {@link Segment} directly instead of hooking into Lucene's IndexWriter warmer
 * callback (which DFA does not use).
 *
 * <p>Parameter type is {@code org.opensearch.index.engine.exec.Segment} (DFA catalog segment),
 * not {@code org.opensearch.index.engine.Segment} (stats DTO).
 */
@ExperimentalApi
public class DataFormatAwareMergeCheckpointPublisher {

    private final ClusterService clusterService;
    private final RecoverySettings recoverySettings;
    private final IndexShard indexShard;
    private final MergedSegmentTransferTracker mergedSegmentTransferTracker;
    private final Logger logger;

    public DataFormatAwareMergeCheckpointPublisher(
        ClusterService clusterService,
        RecoverySettings recoverySettings,
        IndexShard indexShard
    ) {
        this.clusterService = clusterService;
        this.recoverySettings = recoverySettings;
        this.indexShard = indexShard;
        this.mergedSegmentTransferTracker = indexShard.mergedSegmentTransferTracker();
        this.logger = Loggers.getLogger(getClass(), indexShard.shardId());
    }

    /**
     * Entry point. Mirrors {@link MergedSegmentWarmer#warm} — guards, metrics, try/catch/finally.
     * Failures are logged and swallowed so callers can never fail a merge because of publish.
     */
    public void publishCheckpoint(Segment mergedSegment) {
        long startTime = System.currentTimeMillis();
        long elapsedTime = -1;
        boolean shouldPublish = false;
        try {
            long sizeBytes = segmentSize(mergedSegment);
            shouldPublish = shouldPublish(sizeBytes);
            if (shouldPublish == false) {
                return;
            }
            mergedSegmentTransferTracker.incrementTotalWarmInvocationsCount();
            mergedSegmentTransferTracker.incrementOngoingWarms();
            logger.trace(
                () -> new ParameterizedMessage(
                    "Publishing merged segment checkpoint gen={} size={}B",
                    mergedSegment.generation(),
                    sizeBytes
                )
            );
            publishMergedSegment(mergedSegment);
            elapsedTime = System.currentTimeMillis() - startTime;
            long finalElapsed = elapsedTime;
            logger.trace(
                () -> new ParameterizedMessage(
                    "Completed merged segment publish gen={}. Timing: {}ms",
                    mergedSegment.generation(),
                    finalElapsed
                )
            );
        } catch (Throwable t) {
            logger.warn(
                () -> new ParameterizedMessage("Failed to publish merged segment gen={}. Continuing.", mergedSegment.generation()),
                t
            );
            mergedSegmentTransferTracker.incrementTotalWarmFailureCount();
        } finally {
            if (shouldPublish) {
                if (elapsedTime == -1) {
                    elapsedTime = System.currentTimeMillis() - startTime;
                }
                mergedSegmentTransferTracker.addTotalWarmTimeMillis(elapsedTime);
                mergedSegmentTransferTracker.decrementOngoingWarms();
            }
        }
    }

    // package-private for tests
    boolean shouldPublish(long segmentSizeBytes) {
        Version minNodeVersion = clusterService.state().nodes().getMinNodeVersion();
        if (Version.V_3_4_0.compareTo(minNodeVersion) > 0) {
            return false;
        }
        if (recoverySettings.isMergedSegmentReplicationWarmerEnabled() == false) {
            return false;
        }
        long threshold = recoverySettings.getMergedSegmentWarmerMinSegmentSizeThreshold().getBytes();
        if (segmentSizeBytes < threshold) {
            logger.trace(
                () -> new ParameterizedMessage(
                    "Skipping merged segment publish. Size {}B is less than threshold {}B.",
                    segmentSizeBytes,
                    threshold
                )
            );
            return false;
        }
        return true;
    }

    private void publishMergedSegment(Segment mergedSegment) throws IOException {
        assert indexShard.getMergedSegmentPublisher() != null;
        indexShard.getMergedSegmentPublisher().publish(indexShard, computeCheckpoint(mergedSegment));
    }

    private MergedSegmentCheckpoint computeCheckpoint(Segment mergedSegment) throws IOException {
        Collection<String> segmentFiles = flattenSegmentFiles(mergedSegment);
        String segmentName = String.valueOf(mergedSegment.generation());
        try (GatedCloseable<CatalogSnapshot> snapshotCloseable = indexShard.getCatalogSnapshot()) {
            CatalogSnapshot snapshot = snapshotCloseable.get();
            Map<String, StoreFileMetadata> metadata = indexShard.store().getFileMetadata(segmentFiles);
            return new MergedSegmentCheckpoint(
                indexShard.shardId(),
                indexShard.getOperationPrimaryTerm(),
                snapshot.getVersion(),
                metadata.values().stream().mapToLong(StoreFileMetadata::length).sum(),
                indexShard.getCodecName(),
                metadata,
                segmentName
            );
        }
    }

    private static long segmentSize(Segment mergedSegment) {
        return mergedSegment.dfGroupedSearchableFiles().values().stream().mapToLong(WriterFileSet::getTotalSize).sum();
    }

    /** Flattens to format-prefixed names (e.g. {@code "parquet/_5.pqt"}) so {@code FileMetadata#parseDataFormat} routes correctly. */
    private static Collection<String> flattenSegmentFiles(Segment mergedSegment) {
        List<String> fileNames = new ArrayList<>();
        for (Map.Entry<String, WriterFileSet> entry : mergedSegment.dfGroupedSearchableFiles().entrySet()) {
            String formatName = entry.getKey();
            for (String file : entry.getValue().files()) {
                fileNames.add(FileMetadata.serialize(formatName, file));
            }
        }
        return Collections.unmodifiableList(fileNames);
    }
}
