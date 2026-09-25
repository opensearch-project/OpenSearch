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

package org.opensearch.search.profile;

import org.opensearch.Version;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.profile.aggregation.AggregationProfileShardResult;
import org.opensearch.search.profile.aggregation.AggregationProfileShardResultTests;
import org.opensearch.search.profile.fetch.FetchProfileShardResult;
import org.opensearch.search.profile.query.QueryProfileShardResult;
import org.opensearch.search.profile.query.QueryProfileShardResultTests;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

import static org.opensearch.core.xcontent.XContentHelper.toXContent;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureFieldName;
import static org.opensearch.test.XContentTestUtils.insertRandomFields;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertToXContentEquivalent;

public class SearchProfileShardResultsTests extends OpenSearchTestCase {

    public static SearchProfileShardResults createTestItem() {
        int size = rarely() ? 0 : randomIntBetween(1, 2);
        long inboundTime = 0;
        long outboundTime = 0;
        Map<String, ProfileShardResult> searchProfileResults = new HashMap<>(size);
        for (int i = 0; i < size; i++) {
            List<QueryProfileShardResult> queryProfileResults = new ArrayList<>();
            int queryItems = rarely() ? 0 : randomIntBetween(1, 2);
            for (int q = 0; q < queryItems; q++) {
                queryProfileResults.add(QueryProfileShardResultTests.createTestItem());
            }
            AggregationProfileShardResult aggProfileShardResult = AggregationProfileShardResultTests.createTestItem(1);
            List<ProfileResult> fetchResults = new ArrayList<>();
            int fetchItems = rarely() ? 0 : randomIntBetween(1, 2);
            for (int f = 0; f < fetchItems; f++) {
                fetchResults.add(ProfileResultTests.createTestItem(1, false));
            }
            FetchProfileShardResult fetchProfileShardResult = new FetchProfileShardResult(fetchResults);
            NetworkTime networkTime = new NetworkTime(inboundTime, outboundTime);
            long queueWaitNanos = randomBoolean() ? -1L : randomNonNegativeLong();
            searchProfileResults.put(
                randomAlphaOfLengthBetween(5, 10),
                new ProfileShardResult(queryProfileResults, aggProfileShardResult, fetchProfileShardResult, networkTime, queueWaitNanos)
            );
        }
        return new SearchProfileShardResults(searchProfileResults);
    }

    public void testFromXContent() throws IOException {
        doFromXContentTestWithRandomFields(false);
    }

    /**
     * This test adds random fields and objects to the xContent rendered out to ensure we can parse it
     * back to be forward compatible with additions to the xContent
     */
    public void testFromXContentWithRandomFields() throws IOException {
        doFromXContentTestWithRandomFields(true);
    }

    private void doFromXContentTestWithRandomFields(boolean addRandomFields) throws IOException {
        SearchProfileShardResults shardResult = createTestItem();
        XContentType xContentType = randomFrom(XContentType.values());
        boolean humanReadable = randomBoolean();
        BytesReference originalBytes = toShuffledXContent(shardResult, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);
        BytesReference mutated;
        if (addRandomFields) {
            // The ProfileResults "breakdown" section just consists of key/value pairs, we shouldn't add anything random there
            // also we don't want to insert into the root object here, its just the PROFILE_FIELD itself
            Predicate<String> excludeFilter = (s) -> s.isEmpty()
                || s.endsWith(ProfileResult.BREAKDOWN.getPreferredName())
                || s.endsWith(ProfileResult.DEBUG.getPreferredName());
            mutated = insertRandomFields(xContentType, originalBytes, excludeFilter, random());
        } else {
            mutated = originalBytes;
        }
        SearchProfileShardResults parsed;
        try (XContentParser parser = createParser(xContentType.xContent(), mutated)) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            ensureFieldName(parser, parser.nextToken(), SearchProfileShardResults.PROFILE_FIELD);
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            parsed = SearchProfileShardResults.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
            assertNull(parser.nextToken());
        }
        assertToXContentEquivalent(originalBytes, toXContent(parsed, xContentType, humanReadable), xContentType);

    }

    public void testFromXContentWithoutFetchSection() throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.startObject(SearchProfileShardResults.PROFILE_FIELD);
        builder.startArray("shards");
        builder.startObject();
        builder.field("id", "shard1");
        builder.field(SearchProfileShardResults.INBOUND_NETWORK_FIELD, 0);
        builder.field(SearchProfileShardResults.OUTBOUND_NETWORK_FIELD, 0);
        builder.startArray("searches");
        builder.endArray();
        builder.startArray(AggregationProfileShardResult.AGGREGATIONS);
        builder.endArray();
        // no fetch section
        builder.endObject();
        builder.endArray();
        builder.endObject();
        builder.endObject();

        SearchProfileShardResults parsed;
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, BytesReference.bytes(builder))) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            ensureFieldName(parser, parser.nextToken(), SearchProfileShardResults.PROFILE_FIELD);
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            parsed = SearchProfileShardResults.fromXContent(parser);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertEquals(XContentParser.Token.END_OBJECT, parser.nextToken());
            assertNull(parser.nextToken());
        }

        assertEquals(1, parsed.getShardResults().size());
        ProfileShardResult shardResult = parsed.getShardResults().get("shard1");
        assertNotNull(shardResult);
        assertTrue(shardResult.getFetchProfileResult().getFetchProfileResults().isEmpty());
        // a response from a node that does not report queue wait parses as unknown rather than zero
        assertEquals(-1L, shardResult.getQueueWaitNanos());
    }

    public void testQueueWaitSurvivesXContentRoundTrip() throws IOException {
        SearchProfileShardResults results = new SearchProfileShardResults(Map.of("shard1", profileShardResult(123456789L)));

        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference bytes = toShuffledXContent(results, xContentType, ToXContent.EMPTY_PARAMS, randomBoolean());

        SearchProfileShardResults parsed;
        try (XContentParser parser = createParser(xContentType.xContent(), bytes)) {
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            ensureFieldName(parser, parser.nextToken(), SearchProfileShardResults.PROFILE_FIELD);
            ensureExpectedToken(XContentParser.Token.START_OBJECT, parser.nextToken(), parser);
            parsed = SearchProfileShardResults.fromXContent(parser);
        }

        assertEquals(123456789L, parsed.getShardResults().get("shard1").getQueueWaitNanos());
    }

    public void testQueueWaitSurvivesWireRoundTrip() throws IOException {
        long queueWaitNanos = randomBoolean() ? -1L : randomNonNegativeLong();
        assertEquals(queueWaitNanos, copyWithVersion(profileShardResult(queueWaitNanos), Version.CURRENT).getQueueWaitNanos());
    }

    public void testQueueWaitIsOmittedForPreviousVersions() throws IOException {
        // a 3.9 node talking to an older node must not write the field, and the older shape reads back as unknown
        assertEquals(-1L, copyWithVersion(profileShardResult(500L), Version.V_3_8_0).getQueueWaitNanos());
    }

    private static ProfileShardResult profileShardResult(long queueWaitNanos) {
        return new ProfileShardResult(
            new ArrayList<>(),
            new AggregationProfileShardResult(new ArrayList<>()),
            new FetchProfileShardResult(new ArrayList<>()),
            new NetworkTime(0, 0),
            queueWaitNanos
        );
    }

    private static ProfileShardResult copyWithVersion(ProfileShardResult original, Version version) throws IOException {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(version);
            original.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setVersion(version);
                return new ProfileShardResult(in);
            }
        }
    }

}
