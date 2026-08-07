/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.action.search;

import org.opensearch.action.OriginalIndices;
import org.opensearch.cluster.routing.PlainShardIterator;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.Countable;
import org.opensearch.common.util.PlainIterator;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.internal.ShardSearchContextId;

import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * Extension of {@link PlainShardIterator} used in the search api, which also holds the {@link OriginalIndices}
 * of the search request (useful especially with cross-cluster search, as each cluster has its own set of original indices) as well as
 * the cluster alias.
 * @see OriginalIndices
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class SearchShardIterator implements Comparable<SearchShardIterator>, Countable {

    private final OriginalIndices originalIndices;
    private final String clusterAlias;
    private final ShardId shardId;
    private boolean skip = false;

    private final ShardSearchContextId searchContextId;
    private final TimeValue searchContextKeepAlive;
    private final PlainIterator<String> targetNodesIterator;
    @Nullable
    private final List<ShardRouting> shardRoutings;
    private final boolean includeInShardInfo;

    /**
     * Creates a {@link PlainShardIterator} instance that iterates over a subset of the given shards
     * this the a given <code>shardId</code>.
     *
     * @param clusterAlias    the alias of the cluster where the shard is located
     * @param shardId         shard id of the group
     * @param shards          shards to iterate
     * @param originalIndices the indices that the search request originally related to (before any rewriting happened)
     */
    public SearchShardIterator(@Nullable String clusterAlias, ShardId shardId, List<ShardRouting> shards, OriginalIndices originalIndices) {
        this(clusterAlias, shardId, shards, originalIndices, false);
    }

    /**
     * Same as {@link #SearchShardIterator(String, ShardId, List, OriginalIndices)}, additionally marking this shard as
     * participating in the opt-in {@code shard_info} response section. Participation is opt-in for two reasons: the
     * given routings are retained for the lifetime of the search so that the primary flag and shard state can be
     * reported, which for a remote cluster's shards would otherwise become collectable as soon as the iterators are
     * built; and a cluster older than the feature must not be described at all, even when this node resolved its shards
     * itself and could describe them accurately.
     *
     * @param clusterAlias       the alias of the cluster where the shard is located
     * @param shardId            shard id of the group
     * @param shards             shards to iterate
     * @param originalIndices    the indices that the search request originally related to (before any rewriting happened)
     * @param includeInShardInfo whether this shard is reported in {@code shard_info}, and its routings retained
     */
    public SearchShardIterator(
        @Nullable String clusterAlias,
        ShardId shardId,
        List<ShardRouting> shards,
        OriginalIndices originalIndices,
        boolean includeInShardInfo
    ) {
        this(
            clusterAlias,
            shardId,
            shards.stream().map(ShardRouting::currentNodeId).collect(Collectors.toList()),
            includeInShardInfo ? shards : null,
            originalIndices,
            null,
            null,
            includeInShardInfo
        );
    }

    public SearchShardIterator(
        @Nullable String clusterAlias,
        ShardId shardId,
        List<String> targetNodeIds,
        OriginalIndices originalIndices,
        ShardSearchContextId searchContextId,
        TimeValue searchContextKeepAlive
    ) {
        this(clusterAlias, shardId, targetNodeIds, originalIndices, searchContextId, searchContextKeepAlive, false);
    }

    /**
     * Same as {@link #SearchShardIterator(String, ShardId, List, OriginalIndices, ShardSearchContextId, TimeValue)},
     * additionally marking this shard as participating in the opt-in {@code shard_info} response section. Shards
     * targeted through plain node ids have no routings, so such an entry reports neither the primary flag nor the
     * shard state, but it is still reported unless the cluster it belongs to predates the feature.
     *
     * @param includeInShardInfo whether this shard is reported in {@code shard_info}
     */
    public SearchShardIterator(
        @Nullable String clusterAlias,
        ShardId shardId,
        List<String> targetNodeIds,
        OriginalIndices originalIndices,
        ShardSearchContextId searchContextId,
        TimeValue searchContextKeepAlive,
        boolean includeInShardInfo
    ) {
        this(clusterAlias, shardId, targetNodeIds, null, originalIndices, searchContextId, searchContextKeepAlive, includeInShardInfo);
    }

    private SearchShardIterator(
        @Nullable String clusterAlias,
        ShardId shardId,
        List<String> targetNodeIds,
        @Nullable List<ShardRouting> shardRoutings,
        OriginalIndices originalIndices,
        ShardSearchContextId searchContextId,
        TimeValue searchContextKeepAlive,
        boolean includeInShardInfo
    ) {
        this.shardId = shardId;
        this.targetNodesIterator = new PlainIterator<>(targetNodeIds);
        this.shardRoutings = shardRoutings == null ? null : List.copyOf(shardRoutings);
        this.originalIndices = originalIndices;
        this.clusterAlias = clusterAlias;
        this.searchContextId = searchContextId;
        this.searchContextKeepAlive = searchContextKeepAlive;
        this.includeInShardInfo = includeInShardInfo;
        assert searchContextKeepAlive == null || searchContextId != null;
    }

    /**
     * Returns the original indices associated with this shard iterator, specifically with the cluster that this shard belongs to.
     */
    public OriginalIndices getOriginalIndices() {
        return originalIndices;
    }

    /**
     * Returns the alias of the cluster where the shard is located.
     */
    @Nullable
    public String getClusterAlias() {
        return clusterAlias;
    }

    public SearchShardTarget nextOrNull() {
        final String nodeId = targetNodesIterator.nextOrNull();
        if (nodeId != null) {
            return new SearchShardTarget(nodeId, shardId, clusterAlias, originalIndices);
        }
        return null;
    }

    int remaining() {
        return targetNodesIterator.remaining();
    }

    /**
     * Returns a non-null value if this request should use a specific search context instead of the latest one.
     */
    ShardSearchContextId getSearchContextId() {
        return searchContextId;
    }

    TimeValue getSearchContextKeepAlive() {
        return searchContextKeepAlive;
    }

    List<String> getTargetNodeIds() {
        return targetNodesIterator.asList();
    }

    /**
     * Returns the shard routings this iterator was created from, in target order, or {@code null}
     * when they were not retained. They are retained only when this shard participates in
     * {@code shard_info}, so this is {@code null} for an ordinary search even though routings were
     * available. It is also {@code null} whenever the iterator was created from plain node ids,
     * which is how point-in-time readers are resolved, whether they are local or remote; shards of
     * a remote cluster otherwise have routings available, since they are resolved through the
     * {@code _search_shards} API, which reports the remote routing table.
     */
    @Nullable
    List<ShardRouting> getShardRoutings() {
        return shardRoutings;
    }

    /**
     * Returns whether this shard is reported in the opt-in {@code shard_info} response section. It is {@code false}
     * for a search that did not ask for the section, and for the shards of a cluster older than the feature, whose
     * shards are searched as usual but never described.
     */
    boolean includeInShardInfo() {
        return includeInShardInfo;
    }

    /**
     * Reset the iterator and mark it as skippable
     * @see #skip()
     */
    void resetAndSkip() {
        reset();
        skip = true;
    }

    void reset() {
        targetNodesIterator.reset();
    }

    /**
     * Returns <code>true</code> if the search execution should skip this shard since it can not match any documents given the query.
     */
    boolean skip() {
        return skip;
    }

    @Override
    public int size() {
        return targetNodesIterator.size();
    }

    ShardId shardId() {
        return shardId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        SearchShardIterator that = (SearchShardIterator) o;
        return shardId.equals(that.shardId) && Objects.equals(clusterAlias, that.clusterAlias);
    }

    @Override
    public int hashCode() {
        return Objects.hash(clusterAlias, shardId);
    }

    @Override
    public int compareTo(SearchShardIterator o) {
        return Comparator.comparing(SearchShardIterator::shardId)
            .thenComparing(SearchShardIterator::getClusterAlias, Comparator.nullsFirst(String::compareTo))
            .compare(this, o);
    }
}
