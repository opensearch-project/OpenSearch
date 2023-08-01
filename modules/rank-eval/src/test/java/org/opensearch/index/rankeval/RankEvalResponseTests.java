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

import org.opensearch.OpenSearchException;
import org.opensearch.action.OriginalIndices;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.cluster.block.ClusterBlockException;
import org.opensearch.cluster.coordination.NoClusterManagerBlockService;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentLocation;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchParseException;
import org.opensearch.search.SearchShardTarget;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalInt;
import java.util.function.Predicate;

import static java.util.Collections.singleton;
import static org.opensearch.common.xcontent.XContentHelper.toXContent;
import static org.opensearch.test.TestSearchContext.SHARD_TARGET;
import static org.opensearch.test.XContentTestUtils.insertRandomFields;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.Matchers.instanceOf;

public class RankEvalResponseTests extends OpenSearchTestCase {

    private static final Exception[] RANDOM_EXCEPTIONS = new Exception[] {
        new ClusterBlockException(singleton(NoClusterManagerBlockService.NO_CLUSTER_MANAGER_BLOCK_WRITES)),
        new CircuitBreakingException("Data too large", 123, 456, CircuitBreaker.Durability.PERMANENT),
        new SearchParseException(SHARD_TARGET, "Parse failure", new XContentLocation(12, 98)),
        new IllegalArgumentException("Closed resource", new RuntimeException("Resource")),
        new SearchPhaseExecutionException(
            "search",
            "all shards failed",
            new ShardSearchFailure[] {
                new ShardSearchFailure(
                    new ParsingException(1, 2, "foobar", null),
                    new SearchShardTarget("node_1", new ShardId("foo", "_na_", 1), null, OriginalIndices.NONE)
                ) }
        ),
        new OpenSearchException(
            "Parsing failed",
            new ParsingException(9, 42, "Wrong state", new NullPointerException("Unexpected null value"))
        ) };

    private static RankEvalResponse createRandomResponse() {
        int numberOfRequests = randomIntBetween(0, 5);
        Map<String, EvalQueryQuality> partials = new HashMap<>(numberOfRequests);
        for (int i = 0; i < numberOfRequests; i++) {
            String id = randomAlphaOfLengthBetween(3, 10);
            EvalQueryQuality evalQuality = new EvalQueryQuality(id, randomDoubleBetween(0.0, 1.0, true));
            int numberOfDocs = randomIntBetween(0, 5);
            List<RatedSearchHit> ratedHits = new ArrayList<>(numberOfDocs);
            for (int d = 0; d < numberOfDocs; d++) {
                ratedHits.add(searchHit(randomAlphaOfLength(10), randomIntBetween(0, 1000), randomIntBetween(0, 10)));
            }
            evalQuality.addHitsAndRatings(ratedHits);
            partials.put(id, evalQuality);
        }
        int numberOfErrors = randomIntBetween(0, 2);
        Map<String, Exception> errors = new HashMap<>(numberOfRequests);
        for (int i = 0; i < numberOfErrors; i++) {
            errors.put(randomAlphaOfLengthBetween(3, 10), randomFrom(RANDOM_EXCEPTIONS));
        }
        return new RankEvalResponse(randomDouble(), partials, errors);
    }

    public void testSerialization() throws IOException {
        RankEvalResponse randomResponse = createRandomResponse();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            randomResponse.writeTo(output);
            try (StreamInput in = output.bytes().streamInput()) {
                RankEvalResponse deserializedResponse = new RankEvalResponse(in);
                assertEquals(randomResponse.getMetricScore(), deserializedResponse.getMetricScore(), Double.MIN_VALUE);
                assertEquals(randomResponse.getPartialResults(), deserializedResponse.getPartialResults());
                assertEquals(randomResponse.getFailures().keySet(), deserializedResponse.getFailures().keySet());
                assertNotSame(randomResponse, deserializedResponse);
                assertEquals(-1, in.read());
            }
        }
    }

    public void testXContentParsing() throws IOException {
        RankEvalResponse testItem = createRandomResponse();
        boolean humanReadable = randomBoolean();
        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference originalBytes = toShuffledXContent(testItem, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);
        // skip inserting random fields for:
        // - the `details` section, which can contain arbitrary queryIds
        // - everything under `failures` (exceptions parsing is quiet lenient)
        // - everything under `hits` (we test lenient SearchHit parsing elsewhere)
        Predicate<String> pathsToExclude = path -> (path.endsWith("details") || path.contains("failures") || path.contains("hits"));
        BytesReference withRandomFields = insertRandomFields(xContentType, originalBytes, pathsToExclude, random());
        RankEvalResponse parsedItem;
        try (XContentParser parser = createParser(xContentType.xContent(), withRandomFields)) {
            parsedItem = RankEvalResponse.fromXContent(parser);
            assertNull(parser.nextToken());
        }
        assertNotSame(testItem, parsedItem);
        // We cannot check equality of object here because some information (e.g.
        // SearchHit#shard) cannot fully be parsed back.
        assertEquals(testItem.getMetricScore(), parsedItem.getMetricScore(), 0.0);
        assertEquals(testItem.getPartialResults().keySet(), parsedItem.getPartialResults().keySet());
        for (EvalQueryQuality metricDetail : testItem.getPartialResults().values()) {
            EvalQueryQuality parsedEvalQueryQuality = parsedItem.getPartialResults().get(metricDetail.getId());
            assertToXContentEquivalent(
                toXContent(metricDetail, xContentType, humanReadable),
                toXContent(parsedEvalQueryQuality, xContentType, humanReadable),
                xContentType
            );
        }
        // Also exceptions that are parsed back will be different since they are re-wrapped during parsing.
        // However, we can check that there is the expected number
        assertEquals(testItem.getFailures().keySet(), parsedItem.getFailures().keySet());
        for (String queryId : testItem.getFailures().keySet()) {
            Exception ex = parsedItem.getFailures().get(queryId);
            assertThat(ex, instanceOf(OpenSearchException.class));
        }
    }

    public void testToXContent() throws IOException {
        EvalQueryQuality coffeeQueryQuality = new EvalQueryQuality("coffee_query", 0.1);
        coffeeQueryQuality.addHitsAndRatings(Arrays.asList(searchHit("index", 123, 5), searchHit("index", 456, null)));
        RankEvalResponse response = new RankEvalResponse(
            0.123,
            Collections.singletonMap("coffee_query", coffeeQueryQuality),
            Collections.singletonMap("beer_query", new ParsingException(new XContentLocation(0, 0), "someMsg"))
        );
        XContentBuilder builder = MediaTypeRegistry.contentBuilder(XContentType.JSON);
        String xContent = BytesReference.bytes(response.toXContent(builder, ToXContent.EMPTY_PARAMS)).utf8ToString();
        assertEquals(
            ("{"
                + "    \"metric_score\": 0.123,"
                + "    \"details\": {"
                + "        \"coffee_query\": {"
                + "            \"metric_score\": 0.1,"
                + "            \"unrated_docs\": [{\"_index\":\"index\",\"_id\":\"456\"}],"
                + "            \"hits\":[{\"hit\":{\"_index\":\"index\",\"_id\":\"123\",\"_score\":1.0},"
                + "                       \"rating\":5},"
                + "                      {\"hit\":{\"_index\":\"index\",\"_id\":\"456\",\"_score\":1.0},"
                + "                       \"rating\":null}"
                + "                     ]"
                + "        }"
                + "    },"
                + "    \"failures\": {"
                + "        \"beer_query\": {"
                + "          \"error\" : {\"root_cause\": [{\"type\":\"parsing_exception\", \"reason\":\"someMsg\",\"line\":0,\"col\":0}],"
                + "                       \"type\":\"parsing_exception\","
                + "                       \"reason\":\"someMsg\","
                + "                       \"line\":0,\"col\":0"
                + "                      }"
                + "        }"
                + "    }"
                + "}").replaceAll("\\s+", ""),
            xContent
        );
    }

    private static RatedSearchHit searchHit(String index, int docId, Integer rating) {
        SearchHit hit = new SearchHit(docId, docId + "", Collections.emptyMap(), Collections.emptyMap());
        hit.shard(new SearchShardTarget("testnode", new ShardId(index, "uuid", 0), null, OriginalIndices.NONE));
        hit.score(1.0f);
        return new RatedSearchHit(hit, rating != null ? OptionalInt.of(rating) : OptionalInt.empty());
    }
}
