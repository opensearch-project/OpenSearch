/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.remote.RemoteTranslogTransferTracker;
import org.opensearch.indices.RemoteStoreSettings;

/**
 * Factory to provide translog transfer manager to transfer Set of {@link TranslogCheckpointSnapshot}
 *
 * @opensearch.internal
 */
public class TranslogTransferManagerFactory {

    public static BaseTranslogTransferManager getTranslogTransferManager(
        ShardId shardId,
        TransferService transferService,
        BlobPath remoteDataTransferPath,
        BlobPath remoteMetadataTransferPath,
        FileTransferTracker fileTransferTracker,
        RemoteTranslogTransferTracker remoteTranslogTransferTracker,
        RemoteStoreSettings remoteStoreSettings,
        boolean shouldUploadTranslogCkpAsMetadata
    ) {
        if (shouldUploadTranslogCkpAsMetadata) {
            return new TranslogCkpAsMetadataFileTransferManager(
                shardId,
                transferService,
                remoteDataTransferPath,
                remoteMetadataTransferPath,
                fileTransferTracker,
                remoteTranslogTransferTracker,
                remoteStoreSettings
            );
        } else {
            return new TranslogCkpFilesTransferManager(
                shardId,
                transferService,
                remoteDataTransferPath,
                remoteMetadataTransferPath,
                fileTransferTracker,
                remoteTranslogTransferTracker,
                remoteStoreSettings
            );
        }
    }
}
