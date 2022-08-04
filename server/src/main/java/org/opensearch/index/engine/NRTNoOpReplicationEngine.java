/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.translog.NoOpTranslogManager;
import org.opensearch.index.translog.Translog;
import org.opensearch.index.translog.TranslogManager;
import org.opensearch.index.translog.TranslogStats;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * This is an {@link Engine} implementation intended for replica shards when Segment Replication and Remote Store both
 * are enabled.
 *
 * @opensearch.internal
 */
public class NRTNoOpReplicationEngine extends NRTReplicationEngine {

    private static final TranslogStats DEFAULT_STATS = new TranslogStats();
    private final LongSupplier globalCheckpointSupplier;
    private final AtomicLong lastSyncedGlobalCheckpoint = new AtomicLong();

    public NRTNoOpReplicationEngine(EngineConfig engineConfig) {
        super(engineConfig);
        globalCheckpointSupplier = engineConfig.getGlobalCheckpointSupplier();
    }

    @Override
    protected TranslogManager getTranslogManagerRef(EngineConfig engineConfig, String translogUUID) throws IOException {

        return new NoOpTranslogManager(shardId, readLock, this::ensureOpen, null, new Translog.Snapshot() {
            @Override
            public void close() {
            }

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
        return globalCheckpointSupplier.getAsLong() >= 0 ? globalCheckpointSupplier.getAsLong() : getLocalCheckpointTracker().getMaxSeqNo();
    }

    @Override
    public void onSettingsChanged(TimeValue translogRetentionAge, ByteSizeValue translogRetentionSize, long softDeletesRetentionOps) {

    }
}
