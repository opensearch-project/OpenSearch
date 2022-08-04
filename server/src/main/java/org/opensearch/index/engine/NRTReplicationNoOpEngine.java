/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.util.SetOnce;
import org.opensearch.index.translog.NoOpTranslogManager;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogManager;
import org.opensearch.index.translog.TranslogStats;

import java.io.IOException;

/**
 * This is an {@link Engine} implementation intended for replica shards when Segment Replication and remote store
 * are enabled together.
 *
 * @opensearch.internal
 */
public class NRTReplicationNoOpEngine extends AbstractNRTReplicationEngine {

    private static final TranslogStats DEFAULT_STATS = new TranslogStats();

    public NRTReplicationNoOpEngine(EngineConfig engineConfig) {
        super(engineConfig);
    }

    @Override
    protected TranslogManager createTranslogManager(String translogUUID, SetOnce<TranslogManager> translogManager) throws IOException {
        return new NoOpTranslogManager(shardId, readLock, this::ensureOpen, null, new Translog.Snapshot() {
            @Override
            public void close() {}

            @Override
            public int totalOperations() {
                return 0;
            }

            @Override
            public Translog.Operation next() {
                return null;
            }
        }) {
            @Override
            public TranslogStats getTranslogStats() {
                return DEFAULT_STATS;
            }
        };
    }

    @Override
    public long getLastSyncedGlobalCheckpoint() {
        return 0;
    }

    @Override
    public long getPersistedLocalCheckpoint() {
        return getLocalCheckpointTracker().getMaxSeqNo();
    }

    @Override
    public IndexResult index(Index index) throws IOException {
        return super.index(index);
    }
}
