/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.OriginalIndices;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.InternalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.tasks.TaskId;
import org.opensearch.search.SearchSortValuesAndFormats;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A node-level search request.
 *
 * @opensearch.internal
 */
@InternalApi
public class NodeSearchRequest extends TransportRequest {

    private final OriginalIndices originalIndices;
    private final SearchRequest searchRequest;
    private final int totalShardsAcrossAllNodes;
    private final long nowInMillis;
    @Nullable
    private final SearchSortValuesAndFormats bottomSortValues;
    private final List<ShardId> shardIds;
    // Keep consistent with the ShardSearchRequest, these settings are obtained from the coordinator.
    private final List<AliasFilter> aliasFilters;
    private final float[] indexBoosts;
    private final List<String[]> indexRoutings;
    private final int[] indexMaterialByShard;

    public NodeSearchRequest(
        OriginalIndices originalIndices,
        SearchRequest searchRequest,
        int totalShardsAcrossAllNodes,
        long nowInMillis,
        @Nullable SearchSortValuesAndFormats bottomSortValues,
        List<ShardId> shardIds,
        List<AliasFilter> aliasFilters,
        float[] indexBoosts,
        List<String[]> indexRoutings
    ) {
        assert aliasFilters.size() == indexBoosts.length && aliasFilters.size() == indexRoutings.size()
            : "node search index materials must have the same size";
        this.originalIndices = originalIndices;
        this.searchRequest = searchRequest;
        this.totalShardsAcrossAllNodes = totalShardsAcrossAllNodes;
        this.nowInMillis = nowInMillis;
        this.bottomSortValues = bottomSortValues;
        this.shardIds = shardIds;
        this.aliasFilters = aliasFilters;
        this.indexBoosts = indexBoosts;
        this.indexRoutings = indexRoutings;
        this.indexMaterialByShard = buildIndexMaterialByShard(shardIds, aliasFilters.size());
    }

    public NodeSearchRequest(StreamInput in) throws IOException {
        super(in);
        this.originalIndices = OriginalIndices.readOriginalIndices(in);
        this.searchRequest = new SearchRequest(in);
        this.totalShardsAcrossAllNodes = in.readVInt();
        this.nowInMillis = in.readVLong();
        this.bottomSortValues = in.readOptionalWriteable(SearchSortValuesAndFormats::new);
        final int shardCount = in.readVInt();
        this.shardIds = new ArrayList<>(shardCount);
        for (int i = 0; i < shardCount; i++) {
            shardIds.add(new ShardId(in));
        }
        final int indexCount = in.readVInt();
        this.aliasFilters = new ArrayList<>(indexCount);
        this.indexBoosts = new float[indexCount];
        this.indexRoutings = new ArrayList<>(indexCount);
        for (int i = 0; i < indexCount; i++) {
            aliasFilters.add(new AliasFilter(in));
            indexBoosts[i] = in.readFloat();
            indexRoutings.add(in.readStringArray());
        }
        this.indexMaterialByShard = buildIndexMaterialByShard(shardIds, aliasFilters.size());
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        OriginalIndices.writeOriginalIndices(originalIndices, out);
        searchRequest.writeTo(out);
        out.writeVInt(totalShardsAcrossAllNodes);
        out.writeVLong(nowInMillis);
        out.writeOptionalWriteable(bottomSortValues);
        out.writeVInt(shardIds.size());
        for (ShardId shardId : shardIds) {
            shardId.writeTo(out);
        }
        out.writeVInt(aliasFilters.size());
        for (int i = 0; i < aliasFilters.size(); i++) {
            aliasFilters.get(i).writeTo(out);
            out.writeFloat(indexBoosts[i]);
            out.writeStringArray(indexRoutings.get(i));
        }
    }

    @Override
    public Task createTask(long id, String type, String action, TaskId parentTaskId, Map<String, String> headers) {
        return new SearchShardTask(id, type, action, "node_search[shards=" + shardCount() + "]", parentTaskId, headers);
    }

    public int shardCount() {
        return shardIds == null ? 0 : shardIds.size();
    }

    public ShardSearchRequest shardRequest(int index) {
        final ShardId shardId = shardIds.get(index);
        final int indexMaterial = indexMaterialByShard[index];
        if (indexMaterial >= aliasFilters.size()) {
            throw new IllegalStateException("missing index material for shard [" + shardId + "]");
        }
        final ShardSearchRequest shardRequest = new ShardSearchRequest(
            originalIndices,
            this.searchRequest,
            shardId,
            totalShardsAcrossAllNodes,
            aliasFilters.get(indexMaterial),
            indexBoosts[indexMaterial],
            nowInMillis,
            null,
            indexRoutings.get(indexMaterial),
            null,
            null
        );
        shardRequest.setBottomSortValues(bottomSortValues);
        shardRequest.setInboundNetworkTime(nowInMillis);
        return shardRequest;
    }

    private static int[] buildIndexMaterialByShard(List<ShardId> shardIds, int indexMaterialCount) {
        final int[] indexMaterialByShard = new int[shardIds.size()];
        final Map<String, Integer> indexMaterialByUuid = new HashMap<>();
        for (int i = 0; i < shardIds.size(); i++) {
            final String indexUuid = shardIds.get(i).getIndex().getUUID();
            Integer indexMaterial = indexMaterialByUuid.computeIfAbsent(indexUuid, k -> indexMaterialByUuid.size());
            indexMaterialByShard[i] = indexMaterial;
        }
        assert indexMaterialByUuid.size() == indexMaterialCount : "node search index materials must match unique shard indices";
        return indexMaterialByShard;
    }
}
