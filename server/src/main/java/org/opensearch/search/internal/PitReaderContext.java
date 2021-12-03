/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.internal;

import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.index.IndexService;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.Segment;
import org.opensearch.index.shard.IndexShard;

import java.util.List;

public class PitReaderContext extends ReaderContext {

    public ShardRouting getShardRouting() {
        return shardRouting;
    }

    private final ShardRouting shardRouting;
    private final List<Segment> segments;

    public PitReaderContext(ShardSearchContextId id, IndexService indexService,
                            IndexShard indexShard, Engine.SearcherSupplier searcherSupplier,
                            long keepAliveInMillis, boolean singleSession,
                            ShardRouting shardRouting, List<Segment> nonVerboseSegments) {
        super(id, indexService, indexShard, searcherSupplier, keepAliveInMillis, singleSession);
        this.shardRouting = shardRouting;
        segments = nonVerboseSegments;
    }

    public List<Segment> getSegments() {
        return segments;
    }


}
