/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search.scan;

import org.opensearch.common.Nullable;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexShard;

/**
 * Aborts the scans of a closing shard, e.g. one that relocates or whose index is deleted, so they release the shard's
 * reader. A scan that computes stops at once. A scan that waits for the coordinator stops when it resumes or its wait
 * times out.
 */
public final class ArrowScanIndexListener implements IndexEventListener {

    private final ArrowScanRegistry registry;

    /**
     * Creates a listener that aborts the scans of {@code registry}.
     *
     * @param registry the scans of this data node
     */
    public ArrowScanIndexListener(ArrowScanRegistry registry) {
        this.registry = registry;
    }

    @Override
    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        registry.abortShard(shardId, "shard " + shardId + " is closing");
    }
}
