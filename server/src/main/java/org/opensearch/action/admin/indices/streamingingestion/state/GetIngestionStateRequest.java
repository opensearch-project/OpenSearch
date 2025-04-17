/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.streamingingestion.state;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.pagination.PageParams;
import org.opensearch.action.support.broadcast.BroadcastRequest;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.action.ValidateActions.addValidationError;
import static org.opensearch.action.pagination.PageParams.PARAM_ASC_SORT_VALUE;

/**
 * Request to get current ingestion state when using pull-based ingestion. This request supports retrieving index and
 * shard level state. By default, all shards of an index are included.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class GetIngestionStateRequest extends BroadcastRequest<GetIngestionStateRequest> {
    public static final int DEFAULT_PAGE_SIZE = 1000;
    public static final String DEFAULT_SORT_VALUE = PARAM_ASC_SORT_VALUE;

    private int[] shards;
    private PageParams pageParams;

    // holds the <index,shard> pairs to consider when using pagination
    private List<IndexShardPair> indexShardPairsList;

    public GetIngestionStateRequest(String[] indices) {
        super();
        this.indices = indices;
        this.shards = new int[] {};
        this.pageParams = new PageParams(null, DEFAULT_SORT_VALUE, DEFAULT_PAGE_SIZE);
        this.indexShardPairsList = new ArrayList<>();
    }

    public GetIngestionStateRequest(StreamInput in) throws IOException {
        super(in);
        this.shards = in.readVIntArray();
        this.pageParams = in.readOptionalWriteable(PageParams::new);
        this.indexShardPairsList = in.readList(IndexShardPair::new);
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (indices == null) {
            validationException = addValidationError("index is missing", validationException);
        } else if (indices.length != Arrays.stream(indices).collect(Collectors.toSet()).size()) {
            validationException = addValidationError("duplicate index names provided", validationException);
        }
        return validationException;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVIntArray(shards);
        out.writeOptionalWriteable(pageParams);
        out.writeList(indexShardPairsList);
    }

    public int[] getShards() {
        return shards;
    }

    public void setShards(int[] shards) {
        this.shards = shards;
    }

    public PageParams getPageParams() {
        return pageParams;
    }

    public void setPageParams(PageParams pageParams) {
        this.pageParams = pageParams;
    }

    public void addIndexShardPair(String indexName, int shard) {
        indexShardPairsList.add(new IndexShardPair(indexName, shard));
    }

    /**
     * Returns a map of index name and respective shards to be considered.
     */
    public Map<String, Set<Integer>> getIndexShardPairsAsMap() {
        Map<String, Set<Integer>> indexShardMap = new HashMap<>();
        for (IndexShardPair indexShardPair : indexShardPairsList) {
            indexShardMap.computeIfAbsent(indexShardPair.indexName, indexName -> new HashSet<>()).add(indexShardPair.shard);
        }

        return indexShardMap;
    }

    private class IndexShardPair implements Writeable {
        String indexName;
        int shard;

        public IndexShardPair(StreamInput in) throws IOException {
            this.indexName = in.readString();
            this.shard = in.readVInt();
        }

        public IndexShardPair(String indexName, int shard) {
            this.indexName = indexName;
            this.shard = shard;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(indexName);
            out.writeVInt(shard);
        }
    }
}
