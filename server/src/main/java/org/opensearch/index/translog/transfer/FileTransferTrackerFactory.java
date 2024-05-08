/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog.transfer;

import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.remote.RemoteTranslogTransferTracker;

/**
 * A factory class for creating instances of {@link FileTransferTracker}.
 *
 * @opensearch.internal
 */
public class FileTransferTrackerFactory {
    public static FileTransferTracker getFileTransferTracker(
        ShardId shardId,
        RemoteTranslogTransferTracker remoteTranslogTransferTracker,
        boolean ckpAsTranslogMetadata
    ) {
        if (ckpAsTranslogMetadata) {
            return new TranslogCkpAsMetadataFileTransferTracker(shardId, remoteTranslogTransferTracker);
        } else {
            return new TranslogCkpFilesTransferTracker(shardId, remoteTranslogTransferTracker);
        }
    }
}
