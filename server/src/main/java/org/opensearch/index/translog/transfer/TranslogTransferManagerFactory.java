/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.indices.RemoteStoreSettings;

/**
 * Factory to provide translog transfer manager to transfer {@link TranslogCheckpointSnapshot}
 *
 * @opensearch.internal
 */
public class TranslogTransferManagerFactory {

    private final TransferService transferService;
    private final FileTransferTracker fileTransferTracker;
    private final RemoteStoreSettings remoteStoreSettings;
    private final ShardId shardId;

    public TranslogTransferManagerFactory(
        TransferService transferService,
        FileTransferTracker fileTransferTracker,
        RemoteStoreSettings remoteStoreSettings,
        ShardId shardId
    ) {
        this.transferService = transferService;
        this.fileTransferTracker = fileTransferTracker;
        this.remoteStoreSettings = remoteStoreSettings;
        this.shardId = shardId;
    }

    public TranslogCheckpointSnapshotTransferManager getTranslogCheckpointSnapshotTransferManager(boolean isBlobMetadataSupported) {
        if (isBlobMetadataSupported) {
            return new TranslogCheckpointSnapshotTransferManagerWithMetadata(transferService);
        } else {
            return new TranslogCheckpointSnapshotTransferManagerWithoutMetadata(
                transferService,
                fileTransferTracker,
                remoteStoreSettings,
                shardId
            );
        }
    }
}
