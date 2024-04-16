/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.opensearch.common.logging.Loggers;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.remote.RemoteStoreUtils;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.indices.RemoteStoreSettings;

public class CommitTriggeredRemoteSegmentGarbageCollector implements RemoteSegmentGarbageCollector {
    private final Logger logger;
    private final Directory storeDirectory;
    private final RemoteSegmentStoreDirectory remoteDirectory;

    private final RemoteStoreSettings remoteStoreSettings;

    public CommitTriggeredRemoteSegmentGarbageCollector(
        ShardId shardId,
        Directory storeDirectory,
        RemoteSegmentStoreDirectory remoteDirectory,
        RemoteStoreSettings remoteStoreSettings
    ) {
        logger = Loggers.getLogger(getClass(), shardId);
        this.storeDirectory = storeDirectory;
        this.remoteDirectory = remoteDirectory;
        this.remoteStoreSettings = remoteStoreSettings;
    }

    @Override
    public void deleteStaleSegments(Listener listener) {
        // if a new segments_N file is present in local that is not uploaded to remote store yet, it
        // is considered as a first refresh post commit. A cleanup of stale commit files is triggered.
        // This is done to avoid triggering delete post each refresh.
        if (RemoteStoreUtils.isLatestSegmentInfosUploadedToRemote(storeDirectory, remoteDirectory) == false) {
            remoteDirectory.deleteStaleSegmentsAsync(remoteStoreSettings.getMinRemoteSegmentMetadataFiles(), new ActionListener<Void>() {
                @Override
                public void onResponse(Void unused) {
                    listener.onSuccess(unused);
                }

                @Override
                public void onFailure(Exception e) {
                    listener.onFailure(e);
                }
            });
        } else {
            String skipReason =
                "Skipping garbage collection, CommitTriggeredRemoteSegmentGarbageCollector only triggers deletion on commit";
            logger.debug(skipReason);
            listener.onSkip(skipReason);
        }
    }
}
