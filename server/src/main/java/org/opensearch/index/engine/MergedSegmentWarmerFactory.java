/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.IndexWriter;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.recovery.RecoverySettings;
import org.opensearch.transport.TransportService;

/**
 * MergedSegmentWarmerFactory to enable creation of various local on-disk
 * and remote store flavors of {@link IndexWriter.IndexReaderWarmer}
 *
 * @opensearch.internal
 */
@ExperimentalApi
public class MergedSegmentWarmerFactory {
    private final TransportService transportService;
    private final RecoverySettings recoverySettings;
    private final ClusterService clusterService;

    public MergedSegmentWarmerFactory(TransportService transportService, RecoverySettings recoverySettings, ClusterService clusterService) {
        this.transportService = transportService;
        this.recoverySettings = recoverySettings;
        this.clusterService = clusterService;
    }

    public IndexWriter.IndexReaderWarmer get(IndexShard shard) {
        if (shard.indexSettings().isDocumentReplication()) {
            // MergedSegmentWarmerFactory#get is called by IndexShard#newEngineConfig on the initialization of a new indexShard and
            // in case of updates to shard state.
            // - IndexWriter.IndexReaderWarmer should be null when IndexMetadata.INDEX_REPLICATION_TYPE_SETTING == ReplicationType.DOCUMENT
            return null;
        } else if (shard.indexSettings().isSegRepLocalEnabled() || shard.indexSettings().isRemoteStoreEnabled()) {
            return new MergedSegmentWarmer(transportService, recoverySettings, clusterService, shard);
        }
        // We just handle known cases and throw exception at the last. This will allow predictability on the IndexReaderWarmer behaviour.
        throw new IllegalStateException(shard.shardId() + " can't determine IndexReaderWarmer");
    }
}
