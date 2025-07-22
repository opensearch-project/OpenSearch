/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.SegmentCommitInfo;
import org.apache.lucene.index.SegmentReader;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.transport.TransportService;

import java.io.IOException;

/**
 * Implementation of a {@link IndexWriter.IndexReaderWarmer} when local on-disk segment replication is enabled.
 *
 * @opensearch.internal
 */
public class LocalMergedSegmentWarmer implements IndexWriter.IndexReaderWarmer {
    private final TransportService transportService;
    private final RecoverySettings recoverySettings;
    private final ClusterService clusterService;
    private final IndexShard indexShard;

    public LocalMergedSegmentWarmer(
        TransportService transportService,
        RecoverySettings recoverySettings,
        ClusterService clusterService,
        IndexShard indexShard
    ) {
        this.transportService = transportService;
        this.recoverySettings = recoverySettings;
        this.clusterService = clusterService;
        this.indexShard = indexShard;
    }

    @Override
    public void warm(LeafReader leafReader) throws IOException {
        // IndexWriter.IndexReaderWarmer#warm is called by IndexWriter#mergeMiddle. The type of leafReader should be SegmentReader.
        assert leafReader instanceof SegmentReader;

        SegmentCommitInfo segmentCommitInfo = ((SegmentReader) leafReader).getSegmentInfo();
        indexShard.publishMergedSegment(segmentCommitInfo);
    }
}
