/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.OpenSearchException;
import org.opensearch.action.OriginalIndices;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchService;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.FetchSearchResult;
import org.opensearch.search.fetch.QueryFetchSearchResult;
import org.opensearch.search.internal.AliasFilter;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.internal.ShardSearchRequest;
import org.opensearch.search.query.QuerySearchResult;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class NodeSearchRequestTests extends OpenSearchTestCase {

    public void testNodeSearchRequestSerialization() throws Exception {
        final ShardId shardId = new ShardId("index", "uuid", 0);
        final ShardId otherShardId = new ShardId("other-index", "other-uuid", 0);
        final SearchRequest searchRequest = new SearchRequest().searchType(SearchType.QUERY_THEN_FETCH)
            .requestCache(false)
            .allowPartialSearchResults(true)
            .preference("_local")
            .source(new SearchSourceBuilder().size(2));
        final NodeSearchRequest request = new NodeSearchRequest(
            new OriginalIndices(new String[] { "index" }, SearchRequest.DEFAULT_INDICES_OPTIONS),
            searchRequest,
            2,
            123L,
            true,
            null,
            List.of(shardId, otherShardId),
            new int[] { 1, 0 },
            List.of(new AliasFilter(null, "other-alias"), new AliasFilter(null, "alias")),
            new float[] { 3.0f, 2.0f },
            Arrays.asList(new String[] { "other-route" }, new String[] { "route" })
        );

        final NodeSearchRequest copy = copyWriteable(request, writableRegistry(), NodeSearchRequest::new);
        assertThat(copy.shardCount(), equalTo(2));
        final ShardSearchRequest shardRequest = copy.shardRequest(0);
        assertThat(shardRequest.shardId(), equalTo(shardId));
        assertThat(shardRequest.source().size(), equalTo(2));
        assertThat(shardRequest.getAliasFilter(), equalTo(new AliasFilter(null, "alias")));
        assertThat(shardRequest.indexBoost(), equalTo(2.0f));
        assertArrayEquals(new String[] { "route" }, shardRequest.indexRoutings());
        assertThat(shardRequest.numberOfShards(), equalTo(2));
        assertThat(shardRequest.nowInMillis(), equalTo(123L));
        assertTrue(shardRequest.allowPartialSearchResults());
        assertTrue(shardRequest.canReturnNullResponseIfMatchNoDocs());
        assertThat(shardRequest.preference(), equalTo("_local"));

        final ShardSearchRequest otherShardRequest = copy.shardRequest(1);
        assertThat(otherShardRequest.shardId(), equalTo(otherShardId));
        assertThat(otherShardRequest.getAliasFilter(), equalTo(new AliasFilter(null, "other-alias")));
        assertThat(otherShardRequest.indexBoost(), equalTo(3.0f));
        assertArrayEquals(new String[] { "other-route" }, otherShardRequest.indexRoutings());
    }

    public void testNodeSearchResponseSerialization() throws Exception {
        final OpenSearchException failure = new OpenSearchException("boom");
        final NodeSearchResponse<SearchService.CanMatchResponse> response = new NodeSearchResponse<>(
            Arrays.asList(new SearchService.CanMatchResponse(true, null), null),
            Arrays.asList(null, failure)
        );

        final NodeSearchResponse<SearchService.CanMatchResponse> copy = copyWriteable(
            response,
            writableRegistry(),
            NodeSearchResponse::readCanMatch
        );
        assertThat(copy.results().size(), equalTo(2));
        assertThat(copy.failures().size(), equalTo(2));
        assertTrue(copy.results().get(0).canMatch());
        assertNull(copy.failures().get(0));
        assertNull(copy.results().get(1));
        assertThat(copy.failures().get(1).getMessage(), containsString("boom"));
    }

    public void testQueryThenFetchNodeSearchResponseUsesFetchReaderForSingleShard() throws Exception {
        final QueryFetchSearchResult queryFetchResult = new QueryFetchSearchResult(
            QuerySearchResult.nullInstance(),
            fetchResultWithOneHit()
        );
        final NodeSearchResponse<SearchPhaseResult> response = new NodeSearchResponse<>(
            Arrays.asList(queryFetchResult),
            Arrays.asList((Exception) null)
        );

        final BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);

        try (StreamInput in = out.bytes().streamInput()) {
            readWithFixedQueryReader(in);
            assertTrue("fixed QuerySearchResult reader must leave QueryFetchSearchResult fetch bytes unread", in.available() > 0);
        }

        final NodeSearchRequest singleShardRequest = nodeSearchRequest(1);
        try (StreamInput in = out.bytes().streamInput()) {
            final NodeSearchResponse<SearchPhaseResult> copy = NodeSearchResponse.queryThenFetchReader(singleShardRequest).read(in);
            assertThat(copy.results().size(), equalTo(1));
            assertTrue(copy.results().get(0) instanceof QueryFetchSearchResult);
            assertEquals(0, in.available());
        }
    }

    public void testQueryThenFetchNodeSearchResponseUsesQueryReaderForMultipleShards() throws Exception {
        final NodeSearchRequest multiShardRequest = nodeSearchRequest(2);
        final NodeSearchResponse<SearchPhaseResult> response = new NodeSearchResponse<>(
            Arrays.asList(QuerySearchResult.nullInstance()),
            Arrays.asList((Exception) null)
        );

        final BytesStreamOutput out = new BytesStreamOutput();
        response.writeTo(out);

        try (StreamInput in = out.bytes().streamInput()) {
            final NodeSearchResponse<SearchPhaseResult> copy = NodeSearchResponse.queryThenFetchReader(multiShardRequest).read(in);
            assertThat(copy.results().size(), equalTo(1));
            assertTrue(copy.results().get(0) instanceof QuerySearchResult);
            assertEquals(0, in.available());
        }
    }

    private static void readWithFixedQueryReader(StreamInput in) throws Exception {
        assertThat(in.readVInt(), equalTo(1));
        assertTrue(in.readBoolean());
        new QuerySearchResult(in);
    }

    private static FetchSearchResult fetchResultWithOneHit() {
        final SearchShardTarget shardTarget = new SearchShardTarget("node", new ShardId("index", "uuid", 0), null, OriginalIndices.NONE);
        final FetchSearchResult fetchResult = new FetchSearchResult(new ShardSearchContextId("session", 1L), shardTarget);
        fetchResult.hits(new SearchHits(new SearchHit[] { new SearchHit(1) }, null, 1.0f));
        return fetchResult;
    }

    private static NodeSearchRequest nodeSearchRequest(int totalShardsAcrossAllNodes) {
        return new NodeSearchRequest(
            new OriginalIndices(new String[] { "index" }, SearchRequest.DEFAULT_INDICES_OPTIONS),
            new SearchRequest().searchType(SearchType.QUERY_THEN_FETCH),
            totalShardsAcrossAllNodes,
            123L,
            true,
            null,
            List.of(),
            new int[0],
            List.of(),
            new float[0],
            List.of()
        );
    }
}
