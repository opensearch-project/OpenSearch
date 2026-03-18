/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.apache.lucene.search.IndexSearcher;
import org.opensearch.analytics.backend.ShardExecutionContext;
import org.opensearch.index.IndexService;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.shard.IndexShard;

/**
 * Concrete {@link ShardExecutionContext} that wraps an {@link IndexShard}
 * and its parent {@link IndexService}.
 *
 * <p>Back-end plugins that need shard-level resources (e.g., the Lucene
 * backend) downcast the opaque {@link ShardExecutionContext} to this type
 * to access {@link IndexShard} for acquiring searchers and
 * {@link IndexService} for creating {@link QueryShardContext} instances.
 *
 * @opensearch.internal
 */
public class DefaultShardExecutionContext implements ShardExecutionContext {

    private final IndexShard indexShard;
    private final IndexService indexService;

    public DefaultShardExecutionContext(IndexShard indexShard, IndexService indexService) {
        this.indexShard = indexShard;
        this.indexService = indexService;
    }

    @Override
    public String shardId() {
        return indexShard.shardId().toString();
    }

    /** Returns the {@link IndexShard} for acquiring searchers. */
    public IndexShard indexShard() {
        return indexShard;
    }

    /** Returns the {@link IndexService} for creating query contexts. */
    public IndexService indexService() {
        return indexService;
    }

    /**
     * Creates a {@link QueryShardContext} bound to the given searcher.
     *
     * @param searcher the index searcher to bind
     * @return a new QueryShardContext
     */
    public QueryShardContext createQueryShardContext(IndexSearcher searcher) {
        return indexService.newQueryShardContext(
            indexShard.shardId().id(),
            searcher,
            System::currentTimeMillis,
            null
        );
    }
}
