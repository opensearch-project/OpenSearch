/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline;

import org.opensearch.search.SearchShardTarget;

public final class SearchShard {
    public SearchShard(String index, int shardId, String nodeId) {
        this.index = index;
        this.shardId = shardId;
        this.nodeId = nodeId;
    }

    public String getIndex() {
        return index;
    }

    public int getShardId() {
        return shardId;
    }

    public String getNodeId() {
        return nodeId;
    }

    String index;
    int shardId;
    String nodeId;

    /**
     * Create SearchShard from SearchShardTarget
     * @param searchShardTarget
     * @return SearchShard
     */
    public static SearchShard createSearchShard(final SearchShardTarget searchShardTarget) {
        return new SearchShard(searchShardTarget.getIndex(), searchShardTarget.getShardId().id(), searchShardTarget.getNodeId());
    }
}
