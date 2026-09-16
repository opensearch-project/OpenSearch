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

package org.opensearch.index.rankeval;

import org.apache.lucene.search.TotalHits;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchType;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.env.Environment;
import org.opensearch.script.ScriptService;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.node.NodeClient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;

public class TransportRankEvalActionTests extends OpenSearchTestCase {

    private Settings settings = Settings.builder()
        .put("path.home", createTempDir().toString())
        .put("node.name", "test-" + getTestName())
        .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString())
        .build();

    /**
     * Test that request parameters like indicesOptions or searchType from ranking evaluation request are transfered to msearch request
     */
    public void testTransferRequestParameters() throws Exception {
        String indexName = "test_index";
        List<RatedRequest> specifications = new ArrayList<>();
        specifications.add(
            new RatedRequest("amsterdam_query", Arrays.asList(new RatedDocument(indexName, "1", 3)), new SearchSourceBuilder())
        );
        RankEvalRequest rankEvalRequest = new RankEvalRequest(
            new RankEvalSpec(specifications, new DiscountedCumulativeGain()),
            new String[] { indexName }
        );
        SearchType expectedSearchType = randomFrom(SearchType.CURRENTLY_SUPPORTED);
        rankEvalRequest.searchType(expectedSearchType);
        IndicesOptions expectedIndicesOptions = IndicesOptions.fromOptions(
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean(),
            randomBoolean()
        );
        rankEvalRequest.indicesOptions(expectedIndicesOptions);

        NodeClient client = new NodeClient(settings, null) {
            @Override
            public void multiSearch(MultiSearchRequest request, ActionListener<MultiSearchResponse> listener) {
                assertEquals(1, request.requests().size());
                assertEquals(expectedSearchType, request.requests().get(0).searchType());
                assertArrayEquals(new String[] { indexName }, request.requests().get(0).indices());
                assertEquals(expectedIndicesOptions, request.requests().get(0).indicesOptions());
            }
        };

        TransportRankEvalAction action = new TransportRankEvalAction(
            mock(ActionFilters.class),
            client,
            mock(TransportService.class),
            mock(ScriptService.class),
            NamedXContentRegistry.EMPTY
        );
        action.doExecute(null, rankEvalRequest, null);
    }

    /**
     * A hit with a {@code null} _id (as produced today by composite indexes) makes metric evaluation throw an
     * {@link IllegalArgumentException} while joining hits with ratings. This verifies the failure is routed to
     * {@link ActionListener#onFailure} rather than stranding the REST channel (the previous behaviour).
     */
    public void testEvaluationFailureIsPropagatedToListener() {
        String indexName = "test_index";
        RatedRequest specification = new RatedRequest(
            "query_with_null_id_hit",
            Arrays.asList(new RatedDocument(indexName, "1", 3)),
            new SearchSourceBuilder()
        );

        // A hit whose _id is null; joinHitsWithRatings builds a DocumentKey from it and throws.
        SearchHit hitWithNullId = new SearchHit(0, null, Collections.emptyMap(), Collections.emptyMap());
        hitWithNullId.shard(new SearchShardTarget("node", new ShardId(indexName, "uuid", 0), null, OriginalIndices.NONE));
        SearchHits searchHits = new SearchHits(new SearchHit[] { hitWithNullId }, new TotalHits(1, TotalHits.Relation.EQUAL_TO), 1.0f);
        SearchResponse searchResponse = new SearchResponse(
            new InternalSearchResponse(searchHits, null, null, null, false, false, 1),
            null,
            1,
            1,
            0,
            100,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
        MultiSearchResponse multiSearchResponse = new MultiSearchResponse(
            new MultiSearchResponse.Item[] { new MultiSearchResponse.Item(searchResponse, null) },
            100
        );

        AtomicReference<Exception> failure = new AtomicReference<>();
        AtomicReference<RankEvalResponse> success = new AtomicReference<>();
        ActionListener<RankEvalResponse> capturingListener = ActionListener.wrap(success::set, failure::set);

        Map<String, Exception> errors = new HashMap<>();
        TransportRankEvalAction action = new TransportRankEvalAction(
            mock(ActionFilters.class),
            new NodeClient(settings, null),
            mock(TransportService.class),
            mock(ScriptService.class),
            NamedXContentRegistry.EMPTY
        );
        TransportRankEvalAction.RankEvalActionListener listener = action.new RankEvalActionListener(
            capturingListener, new PrecisionAtK(), new RatedRequest[] { specification }, errors
        );

        listener.onResponse(multiSearchResponse);

        assertNull("evaluation must not have produced a response", success.get());
        assertNotNull("evaluation failure must be propagated to the listener", failure.get());
        assertTrue(failure.get() instanceof IllegalArgumentException);
    }
}
