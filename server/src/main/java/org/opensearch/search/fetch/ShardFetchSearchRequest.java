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
 *    http://www.apache.org/licenses/LICENSE-2.0
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
import org.opensearch.search.internal.ShardSearchRequest;

import java.io.IOException;
import java.util.List;

/**
 * Shard level fetch request used with search. Holds indices taken from the original search request
 * and implements {@link org.opensearch.action.IndicesRequest}.
 *
 * @opensearch.internal
 */
public class ShardFetchSearchRequest extends ShardFetchRequest implements IndicesRequest {

    private final OriginalIndices originalIndices;
    private final ShardSearchRequest shardSearchRequest;
    private final RescoreDocIds rescoreDocIds;
    private final AggregatedDfs aggregatedDfs;

    public ShardFetchSearchRequest(
        OriginalIndices originalIndices,
        ShardSearchContextId id,
        ShardSearchRequest shardSearchRequest,
        List<Integer> list,
        ScoreDoc lastEmittedDoc,
        RescoreDocIds rescoreDocIds,
        AggregatedDfs aggregatedDfs
    ) {
        super(id, list, lastEmittedDoc);
        this.originalIndices = originalIndices;
        this.shardSearchRequest = shardSearchRequest;
        this.rescoreDocIds = rescoreDocIds;
        this.aggregatedDfs = aggregatedDfs;
    }

    public ShardFetchSearchRequest(StreamInput in) throws IOException {
        super(in);
        originalIndices = OriginalIndices.readOriginalIndices(in);
        shardSearchRequest = in.readOptionalWriteable(ShardSearchRequest::new);
        rescoreDocIds = new RescoreDocIds(in);
        aggregatedDfs = in.readOptionalWriteable(AggregatedDfs::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        OriginalIndices.writeOriginalIndices(originalIndices, out);
        out.writeOptionalWriteable(shardSearchRequest);
        rescoreDocIds.writeTo(out);
        out.writeOptionalWriteable(aggregatedDfs);
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
    public ShardSearchRequest getShardSearchRequest() {
        return shardSearchRequest;
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
