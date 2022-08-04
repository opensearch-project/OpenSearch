/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.util.SetOnce;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.translog.TranslogDeletionPolicy;
import org.opensearch.index.translog.TranslogException;
import org.opensearch.index.translog.TranslogManager;
import org.opensearch.index.translog.WriteOnlyTranslogManager;
import org.opensearch.index.translog.listener.TranslogEventListener;

import java.io.IOException;

/**
 * This is an {@link Engine} implementation intended for replica shards when Segment Replication
 * is enabled.  This Engine does not create an IndexWriter, rather it refreshes a {@link NRTReplicationReaderManager}
 * with new Segments when received from an external source.
 *
 * @opensearch.internal
 */
public class NRTReplicationEngine extends AbstractNRTReplicationEngine {

    public NRTReplicationEngine(EngineConfig engineConfig) {
        super(engineConfig);
    }

    @Override
    protected TranslogManager createTranslogManager(String translogUUID, SetOnce<TranslogManager> translogManager) throws IOException {
        return new WriteOnlyTranslogManager(
            config().getTranslogConfig(),
            config().getPrimaryTermSupplier(),
            config().getGlobalCheckpointSupplier(),
            getTranslogDeletionPolicy(config()),
            config().getShardId(),
            readLock,
            this::getLocalCheckpointTracker,
            translogUUID,
            new TranslogEventListener() {
                @Override
                public void onFailure(String reason, Exception ex) {
                    failEngine(reason, ex);
                }

                @Override
                public void onAfterTranslogSync() {
                    try {
                        ((WriteOnlyTranslogManager) translogManager.get()).trimUnreferencedReaders();
                    } catch (IOException ex) {
                        throw new TranslogException(shardId, "failed to trim unreferenced translog readers", ex);
                    }
                }
            },
            this
        );
    }

    @Override
    public long getLastSyncedGlobalCheckpoint() {
        return ((WriteOnlyTranslogManager) translogManager()).getLastSyncedGlobalCheckpoint();
    }

    @Override
    public void onSettingsChanged(TimeValue translogRetentionAge, ByteSizeValue translogRetentionSize, long softDeletesRetentionOps) {
        final TranslogDeletionPolicy translogDeletionPolicy = ((WriteOnlyTranslogManager) translogManager()).getDeletionPolicy();
        translogDeletionPolicy.setRetentionAgeInMillis(translogRetentionAge.millis());
        translogDeletionPolicy.setRetentionSizeInBytes(translogRetentionSize.getBytes());
    }
}
