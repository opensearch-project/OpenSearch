/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.fetch;

import org.apache.lucene.search.ScoreDoc;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.RescoreDocIds;
import org.opensearch.search.dfs.AggregatedDfs;
import org.opensearch.search.internal.ProtobufShardSearchRequest;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

/**
 * Shard level fetch request used with search. Holds indices taken from the original search request
 * and implements {@link org.opensearch.action.IndicesRequest}.
 *
 * @opensearch.internal
 */
public class ProtobufShardFetchSearchRequest extends ProtobufShardFetchRequest implements IndicesRequest {

    // TODO: proto message
    private final OriginalIndices originalIndices;
    private final ProtobufShardSearchRequest ProtobufShardSearchRequest;
    private final RescoreDocIds rescoreDocIds;
    private final AggregatedDfs aggregatedDfs;

    public ProtobufShardFetchSearchRequest(
        OriginalIndices originalIndices,
        ShardSearchContextId id,
        ProtobufShardSearchRequest ProtobufShardSearchRequest,
        List<Integer> list,
        ScoreDoc lastEmittedDoc,
        RescoreDocIds rescoreDocIds,
        AggregatedDfs aggregatedDfs
    ) {
        super(id, list, lastEmittedDoc);
        this.originalIndices = originalIndices;
        this.ProtobufShardSearchRequest = ProtobufShardSearchRequest;
        this.rescoreDocIds = rescoreDocIds;
        this.aggregatedDfs = aggregatedDfs;
    }

    public ProtobufShardFetchSearchRequest(byte[] in) throws IOException {
        super(in);
        originalIndices = null;
        ProtobufShardSearchRequest = null;
        rescoreDocIds = null;
        aggregatedDfs = null;
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
        super.writeTo(out);
        // OriginalIndices.writeOriginalIndices(originalIndices, out);
        // out.writeOptionalWriteable(ProtobufShardSearchRequest);
        // rescoreDocIds.writeTo(out);
        // out.writeOptionalWriteable(aggregatedDfs);
    }

    @Override
    public String[] indices() {
        if (originalIndices == null) {
            return null;
        }
        return originalIndices.indices();
    }

    @Override
    public IndicesOptions indicesOptions() {
        if (originalIndices == null) {
            return null;
        }
        return originalIndices.indicesOptions();
    }

    @Override
    public ProtobufShardSearchRequest getProtobufShardSearchRequest() {
        return ProtobufShardSearchRequest;
    }

    @Override
    public RescoreDocIds getRescoreDocIds() {
        return rescoreDocIds;
    }

    @Override
    public AggregatedDfs getAggregatedDfs() {
        return aggregatedDfs;
    }
}
