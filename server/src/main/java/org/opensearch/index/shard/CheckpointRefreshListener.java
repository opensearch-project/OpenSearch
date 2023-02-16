/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.ReferenceManager;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * A {@link ReferenceManager.RefreshListener} that publishes a checkpoint to be consumed by replicas.
 * This class is only used with Segment Replication enabled.
 *
 * @opensearch.internal
 */
public class CheckpointRefreshListener implements ReferenceManager.RefreshListener {

    protected static Logger logger = LogManager.getLogger(CheckpointRefreshListener.class);

    private final IndexShard shard;
    private final Consumer<IndexShard> checkpointUpdateConsumer;

    public CheckpointRefreshListener(IndexShard shard, Consumer<IndexShard> checkpointUpdateConsumer) {
        this.shard = shard;
        this.checkpointUpdateConsumer = checkpointUpdateConsumer;
    }

    @Override
    public void beforeRefresh() throws IOException {
        // Do nothing
    }

    @Override
    public void afterRefresh(boolean didRefresh) throws IOException {
        if (shard.state() != IndexShardState.CLOSED && shard.getReplicationTracker().isPrimaryMode()) {
            checkpointUpdateConsumer.accept(shard);
        }
    }
}
