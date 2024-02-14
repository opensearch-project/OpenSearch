/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.externalengine;

import static org.opensearch.repositories.fs.ReloadableFsRepository.randomIntBetween;

import java.io.IOException;
import java.util.concurrent.ThreadLocalRandom;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.internal.InternalSearchResponse;

public class SQLQueryEngine extends QueryEngine {

    public static final String NAME = "sql";

    public SQLQueryEngine() {

    }
    public SQLQueryEngine(StreamInput in) {
    }
    @Override
    public String getWriteableName() {
        return "sql";
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
    }


    @Override
    public void executeQuery(SearchRequest searchRequest,
                             ActionListener<SearchResponse> actionListener) {
        // Creating a minimal response is OK, because SearchResponse self
        // is tested elsewhere.
        long tookInMillis = ThreadLocalRandom.current().nextLong(0L, Long.MAX_VALUE);
        int totalShards = randomIntBetween(1, Integer.MAX_VALUE);
        int successfulShards = randomIntBetween(0, totalShards);
        int skippedShards = totalShards - successfulShards;
        InternalSearchResponse internalSearchResponse = InternalSearchResponse.empty();
        int totalClusters = randomIntBetween(0, 10);
        int successfulClusters = randomIntBetween(0, totalClusters);
        int skippedClusters = totalClusters - successfulClusters;
        SearchResponse.Clusters clusters = new SearchResponse.Clusters(totalClusters, successfulClusters, skippedClusters);
        SearchResponse searchResponse = new SearchResponse(
            internalSearchResponse,
            null,
            totalShards,
            successfulShards,
            skippedShards,
            tookInMillis,
            ShardSearchFailure.EMPTY_ARRAY,
            clusters
        );
        actionListener.onResponse(searchResponse);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return null;
    }

    public static QueryEngine fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            token = parser.nextToken();
        }
        return new SQLQueryEngine();
    }

}
