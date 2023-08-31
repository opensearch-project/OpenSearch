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

package org.opensearch.action.search;

import org.apache.lucene.search.TotalHits;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentHelper;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.rest.action.search.RestSearchAction;
import org.opensearch.search.GenericSearchExtBuilder;
import org.opensearch.search.SearchExtBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.SearchHitsTests;
import org.opensearch.search.SearchModule;
import org.opensearch.search.aggregations.AggregationsTests;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.profile.SearchProfileShardResults;
import org.opensearch.search.profile.SearchProfileShardResultsTests;
import org.opensearch.search.suggest.Suggest;
import org.opensearch.search.suggest.SuggestTests;
import org.opensearch.test.InternalAggregationTestCase;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

import static java.util.Collections.singletonMap;
import static org.opensearch.test.XContentTestUtils.insertRandomFields;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertToXContentEquivalent;

public class SearchResponseTests extends OpenSearchTestCase {

    private static final NamedXContentRegistry xContentRegistry;
    static {
        List<NamedXContentRegistry.Entry> namedXContents = new ArrayList<>(InternalAggregationTestCase.getDefaultNamedXContents());
        namedXContents.addAll(SuggestTests.getDefaultNamedXContents());
        namedXContents.add(
            new NamedXContentRegistry.Entry(SearchExtBuilder.class, DummySearchExtBuilder.DUMMY_FIELD, DummySearchExtBuilder::parse)
        );
        xContentRegistry = new NamedXContentRegistry(namedXContents);
    }

    private final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(
        new SearchModule(Settings.EMPTY, List.of(new SearchPlugin() {
            @Override
            public List<SearchExtSpec<?>> getSearchExts() {
                return List.of(
                    new SearchExtSpec<>(
                        DummySearchExtBuilder.DUMMY_FIELD,
                        DummySearchExtBuilder::new,
                        parser -> DummySearchExtBuilder.parse(parser)
                    )
                );
            }
        })).getNamedWriteables()
    );
    private AggregationsTests aggregationsTests = new AggregationsTests();

    @Before
    public void init() throws Exception {
        aggregationsTests.init();
    }

    @After
    public void cleanUp() throws Exception {
        aggregationsTests.cleanUp();
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }

    private SearchResponse createTestItem(ShardSearchFailure... shardSearchFailures) {
        return createTestItem(false, shardSearchFailures);
    }

    /**
     * This SearchResponse doesn't include SearchHits, Aggregations, Suggestions, ShardSearchFailures, SearchProfileShardResults
     * to make it possible to only test properties of the SearchResponse itself
     */
    private SearchResponse createMinimalTestItem() {
        return createTestItem(true);
    }

    /**
     * if minimal is set, don't include search hits, aggregations, suggest etc... to make test simpler
     */
    private SearchResponse createTestItem(boolean minimal, ShardSearchFailure... shardSearchFailures) {
        return createTestItem(minimal, Collections.emptyList(), shardSearchFailures);
    }

    public SearchResponse createTestItem(
        boolean minimal,
        List<SearchExtBuilder> searchExtBuilders,
        ShardSearchFailure... shardSearchFailures
    ) {
        boolean timedOut = randomBoolean();
        Boolean terminatedEarly = randomBoolean() ? null : randomBoolean();
        int numReducePhases = randomIntBetween(1, 10);
        long tookInMillis = randomNonNegativeLong();
        int totalShards = randomIntBetween(1, Integer.MAX_VALUE);
        int successfulShards = randomIntBetween(0, totalShards);
        int skippedShards = randomIntBetween(0, totalShards);
        InternalSearchResponse internalSearchResponse;
        if (minimal == false) {
            SearchHits hits = SearchHitsTests.createTestItem(true, true);
            InternalAggregations aggregations = aggregationsTests.createTestInstance();
            Suggest suggest = SuggestTests.createTestItem();
            SearchProfileShardResults profileShardResults = SearchProfileShardResultsTests.createTestItem();
            internalSearchResponse = new InternalSearchResponse(
                hits,
                aggregations,
                suggest,
                profileShardResults,
                timedOut,
                terminatedEarly,
                numReducePhases,
                searchExtBuilders
            );
        } else {
            internalSearchResponse = InternalSearchResponse.empty();
        }

        return new SearchResponse(
            internalSearchResponse,
            null,
            totalShards,
            successfulShards,
            skippedShards,
            tookInMillis,
            shardSearchFailures,
            randomBoolean() ? randomClusters() : SearchResponse.Clusters.EMPTY,
            null
        );
    }

    static SearchResponse.Clusters randomClusters() {
        int totalClusters = randomIntBetween(0, 10);
        int successfulClusters = randomIntBetween(0, totalClusters);
        int skippedClusters = totalClusters - successfulClusters;
        return new SearchResponse.Clusters(totalClusters, successfulClusters, skippedClusters);
    }

    /**
     * the "_shard/total/failures" section makes it impossible to directly
     * compare xContent, so we omit it here
     */
    public void testFromXContent() throws IOException {
        doFromXContentTestWithRandomFields(createTestItem(), false);
    }

    public void testFromXContentWithSearchExtBuilders() throws IOException {
        doFromXContentTestWithRandomFields(createTestItem(false, List.of(new DummySearchExtBuilder(UUID.randomUUID().toString()))), false);
    }

    public void testFromXContentWithUnregisteredSearchExtBuilders() throws IOException {
        List<NamedXContentRegistry.Entry> namedXContents = new ArrayList<>(InternalAggregationTestCase.getDefaultNamedXContents());
        namedXContents.addAll(SuggestTests.getDefaultNamedXContents());
        String dummyId = UUID.randomUUID().toString();
        String fakeId = UUID.randomUUID().toString();
        List<SearchExtBuilder> extBuilders = List.of(new DummySearchExtBuilder(dummyId), new FakeSearchExtBuilder(fakeId));
        SearchResponse response = createTestItem(false, extBuilders);
        MediaType xcontentType = randomFrom(XContentType.values());
        boolean humanReadable = randomBoolean();
        final ToXContent.Params params = new ToXContent.MapParams(singletonMap(RestSearchAction.TYPED_KEYS_PARAM, "true"));
        BytesReference originalBytes = toShuffledXContent(response, xcontentType, params, humanReadable);
        XContentParser parser = createParser(new NamedXContentRegistry(namedXContents), xcontentType.xContent(), originalBytes);
        SearchResponse parsed = SearchResponse.fromXContent(parser);
        assertEquals(extBuilders.size(), response.getInternalResponse().getSearchExtBuilders().size());

        List<SearchExtBuilder> actual = parsed.getInternalResponse().getSearchExtBuilders();
        assertEquals(extBuilders.size(), actual.size());
        for (int i = 0; i < actual.size(); i++) {
            assertTrue(actual.get(0) instanceof GenericSearchExtBuilder);
        }
    }

    /**
     * This test adds random fields and objects to the xContent rendered out to
     * ensure we can parse it back to be forward compatible with additions to
     * the xContent. We test this with a "minimal" SearchResponse, adding random
     * fields to SearchHits, Aggregations etc... is tested in their own tests
     */
    public void testFromXContentWithRandomFields() throws IOException {
        doFromXContentTestWithRandomFields(createMinimalTestItem(), true);
    }

    public void doFromXContentTestWithRandomFields(SearchResponse response, boolean addRandomFields) throws IOException {
        MediaType xcontentType = randomFrom(XContentType.values());
        boolean humanReadable = randomBoolean();
        final ToXContent.Params params = new ToXContent.MapParams(singletonMap(RestSearchAction.TYPED_KEYS_PARAM, "true"));
        BytesReference originalBytes = toShuffledXContent(response, xcontentType, params, humanReadable);
        BytesReference mutated;
        if (addRandomFields) {
            mutated = insertRandomFields(xcontentType, originalBytes, null, random());
        } else {
            mutated = originalBytes;
        }
        try (XContentParser parser = createParser(xcontentType.xContent(), mutated)) {
            SearchResponse parsed = SearchResponse.fromXContent(parser);
            assertToXContentEquivalent(originalBytes, XContentHelper.toXContent(parsed, xcontentType, params, humanReadable), xcontentType);
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }
    }

    /**
     * The "_shard/total/failures" section makes if impossible to directly compare xContent, because
     * the failures in the parsed SearchResponse are wrapped in an extra OpenSearchException on the client side.
     * Because of this, in this special test case we compare the "top level" fields for equality
     * and the subsections xContent equivalence independently
     */
    public void testFromXContentWithFailures() throws IOException {
        int numFailures = randomIntBetween(1, 5);
        ShardSearchFailure[] failures = new ShardSearchFailure[numFailures];
        for (int i = 0; i < failures.length; i++) {
            failures[i] = ShardSearchFailureTests.createTestItem(IndexMetadata.INDEX_UUID_NA_VALUE);
        }
        SearchResponse response = createTestItem(failures);
        XContentType xcontentType = randomFrom(XContentType.values());
        final ToXContent.Params params = new ToXContent.MapParams(singletonMap(RestSearchAction.TYPED_KEYS_PARAM, "true"));
        BytesReference originalBytes = toShuffledXContent(response, xcontentType, params, randomBoolean());
        try (XContentParser parser = createParser(xcontentType.xContent(), originalBytes)) {
            SearchResponse parsed = SearchResponse.fromXContent(parser);
            for (int i = 0; i < parsed.getShardFailures().length; i++) {
                ShardSearchFailure parsedFailure = parsed.getShardFailures()[i];
                ShardSearchFailure originalFailure = failures[i];
                assertEquals(originalFailure.index(), parsedFailure.index());
                assertEquals(originalFailure.shard(), parsedFailure.shard());
                assertEquals(originalFailure.shardId(), parsedFailure.shardId());
                String originalMsg = originalFailure.getCause().getMessage();
                assertEquals(
                    parsedFailure.getCause().getMessage(),
                    "OpenSearch exception [type=parsing_exception, reason=" + originalMsg + "]"
                );
                String nestedMsg = originalFailure.getCause().getCause().getMessage();
                assertEquals(
                    parsedFailure.getCause().getCause().getMessage(),
                    "OpenSearch exception [type=illegal_argument_exception, reason=" + nestedMsg + "]"
                );
            }
            assertEquals(XContentParser.Token.END_OBJECT, parser.currentToken());
            assertNull(parser.nextToken());
        }
    }

    public void testToXContent() {
        SearchHit hit = new SearchHit(1, "id1", Collections.emptyMap(), Collections.emptyMap());
        hit.score(2.0f);
        SearchHit[] hits = new SearchHit[] { hit };
        String dummyId = UUID.randomUUID().toString();
        {
            SearchResponse response = new SearchResponse(
                new InternalSearchResponse(
                    new SearchHits(hits, new TotalHits(100, TotalHits.Relation.EQUAL_TO), 1.5f),
                    null,
                    null,
                    null,
                    false,
                    null,
                    1,
                    List.of(new DummySearchExtBuilder(dummyId))
                ),
                null,
                0,
                0,
                0,
                0,
                ShardSearchFailure.EMPTY_ARRAY,
                SearchResponse.Clusters.EMPTY,
                null
            );
            StringBuilder expectedString = new StringBuilder();
            expectedString.append("{");
            {
                expectedString.append("\"took\":0,");
                expectedString.append("\"timed_out\":false,");
                expectedString.append("\"_shards\":");
                {
                    expectedString.append("{\"total\":0,");
                    expectedString.append("\"successful\":0,");
                    expectedString.append("\"skipped\":0,");
                    expectedString.append("\"failed\":0},");
                }
                expectedString.append("\"hits\":");
                {
                    expectedString.append("{\"total\":{\"value\":100,\"relation\":\"eq\"},");
                    expectedString.append("\"max_score\":1.5,");
                    expectedString.append("\"hits\":[{\"_id\":\"id1\",\"_score\":2.0}]},");
                }
                expectedString.append("\"ext\":");
                {
                    expectedString.append("{\"dummy\":\"" + dummyId + "\"}");
                }
            }
            expectedString.append("}");
            assertEquals(expectedString.toString(), Strings.toString(MediaTypeRegistry.JSON, response));
            List<SearchExtBuilder> searchExtBuilders = response.getInternalResponse().getSearchExtBuilders();
            assertEquals(1, searchExtBuilders.size());
        }
        {
            SearchResponse response = new SearchResponse(
                new InternalSearchResponse(
                    new SearchHits(hits, new TotalHits(100, TotalHits.Relation.EQUAL_TO), 1.5f),
                    null,
                    null,
                    null,
                    false,
                    null,
                    1
                ),
                null,
                0,
                0,
                0,
                0,
                ShardSearchFailure.EMPTY_ARRAY,
                new SearchResponse.Clusters(5, 3, 2)
            );
            StringBuilder expectedString = new StringBuilder();
            expectedString.append("{");
            {
                expectedString.append("\"took\":0,");
                expectedString.append("\"timed_out\":false,");
                expectedString.append("\"_shards\":");
                {
                    expectedString.append("{\"total\":0,");
                    expectedString.append("\"successful\":0,");
                    expectedString.append("\"skipped\":0,");
                    expectedString.append("\"failed\":0},");
                }
                expectedString.append("\"_clusters\":");
                {
                    expectedString.append("{\"total\":5,");
                    expectedString.append("\"successful\":3,");
                    expectedString.append("\"skipped\":2},");
                }
                expectedString.append("\"hits\":");
                {
                    expectedString.append("{\"total\":{\"value\":100,\"relation\":\"eq\"},");
                    expectedString.append("\"max_score\":1.5,");
                    expectedString.append("\"hits\":[{\"_id\":\"id1\",\"_score\":2.0}]}");
                }
            }
            expectedString.append("}");
            assertEquals(expectedString.toString(), Strings.toString(MediaTypeRegistry.JSON, response));
        }
    }

    public void testSerialization() throws IOException {
        SearchResponse searchResponse = createTestItem(false);
        SearchResponse deserialized = copyWriteable(searchResponse, namedWriteableRegistry, SearchResponse::new, Version.CURRENT);
        if (searchResponse.getHits().getTotalHits() == null) {
            assertNull(deserialized.getHits().getTotalHits());
        } else {
            assertEquals(searchResponse.getHits().getTotalHits().value, deserialized.getHits().getTotalHits().value);
            assertEquals(searchResponse.getHits().getTotalHits().relation, deserialized.getHits().getTotalHits().relation);
        }
        assertEquals(searchResponse.getHits().getHits().length, deserialized.getHits().getHits().length);
        assertEquals(searchResponse.getNumReducePhases(), deserialized.getNumReducePhases());
        assertEquals(searchResponse.getFailedShards(), deserialized.getFailedShards());
        assertEquals(searchResponse.getTotalShards(), deserialized.getTotalShards());
        assertEquals(searchResponse.getSkippedShards(), deserialized.getSkippedShards());
        assertEquals(searchResponse.getClusters(), deserialized.getClusters());
    }

    public void testSerializationWithSearchExtBuilders() throws IOException {
        String id = UUID.randomUUID().toString();
        SearchResponse searchResponse = createTestItem(false, List.of(new DummySearchExtBuilder(id)));
        SearchResponse deserialized = copyWriteable(searchResponse, namedWriteableRegistry, SearchResponse::new, Version.CURRENT);
        if (searchResponse.getHits().getTotalHits() == null) {
            assertNull(deserialized.getHits().getTotalHits());
        } else {
            assertEquals(searchResponse.getHits().getTotalHits().value, deserialized.getHits().getTotalHits().value);
            assertEquals(searchResponse.getHits().getTotalHits().relation, deserialized.getHits().getTotalHits().relation);
        }
        assertEquals(searchResponse.getHits().getHits().length, deserialized.getHits().getHits().length);
        assertEquals(searchResponse.getNumReducePhases(), deserialized.getNumReducePhases());
        assertEquals(searchResponse.getFailedShards(), deserialized.getFailedShards());
        assertEquals(searchResponse.getTotalShards(), deserialized.getTotalShards());
        assertEquals(searchResponse.getSkippedShards(), deserialized.getSkippedShards());
        assertEquals(searchResponse.getClusters(), deserialized.getClusters());
        assertEquals(
            searchResponse.getInternalResponse().getSearchExtBuilders().get(0),
            deserialized.getInternalResponse().getSearchExtBuilders().get(0)
        );
    }

    public void testSerializationWithSearchExtBuildersOnUnsupportedWriterVersion() throws IOException {
        String id = UUID.randomUUID().toString();
        SearchResponse searchResponse = createTestItem(false, List.of(new DummySearchExtBuilder(id)));
        SearchResponse deserialized = copyWriteable(searchResponse, namedWriteableRegistry, SearchResponse::new, Version.V_2_9_0);
        if (searchResponse.getHits().getTotalHits() == null) {
            assertNull(deserialized.getHits().getTotalHits());
        } else {
            assertEquals(searchResponse.getHits().getTotalHits().value, deserialized.getHits().getTotalHits().value);
            assertEquals(searchResponse.getHits().getTotalHits().relation, deserialized.getHits().getTotalHits().relation);
        }
        assertEquals(searchResponse.getHits().getHits().length, deserialized.getHits().getHits().length);
        assertEquals(searchResponse.getNumReducePhases(), deserialized.getNumReducePhases());
        assertEquals(searchResponse.getFailedShards(), deserialized.getFailedShards());
        assertEquals(searchResponse.getTotalShards(), deserialized.getTotalShards());
        assertEquals(searchResponse.getSkippedShards(), deserialized.getSkippedShards());
        assertEquals(searchResponse.getClusters(), deserialized.getClusters());
        assertEquals(1, searchResponse.getInternalResponse().getSearchExtBuilders().size());
        assertTrue(deserialized.getInternalResponse().getSearchExtBuilders().isEmpty());
    }

    public void testToXContentEmptyClusters() throws IOException {
        SearchResponse searchResponse = new SearchResponse(
            InternalSearchResponse.empty(),
            null,
            1,
            1,
            0,
            1,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
        SearchResponse deserialized = copyWriteable(searchResponse, namedWriteableRegistry, SearchResponse::new, Version.CURRENT);
        XContentBuilder builder = XContentBuilder.builder(MediaTypeRegistry.JSON.xContent());
        deserialized.getClusters().toXContent(builder, ToXContent.EMPTY_PARAMS);
        assertEquals(0, builder.toString().length());
    }

    static class DummySearchExtBuilder extends SearchExtBuilder {

        static ParseField DUMMY_FIELD = new ParseField("dummy");

        protected final String id;

        public DummySearchExtBuilder(String id) {
            assertNotNull(id);
            this.id = id;
        }

        public DummySearchExtBuilder(StreamInput in) throws IOException {
            this.id = in.readString();
        }

        public String getId() {
            return this.id;
        }

        @Override
        public String getWriteableName() {
            return DUMMY_FIELD.getPreferredName();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(this.id);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.field("dummy", id);
        }

        @Override
        public int hashCode() {
            return 0;
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }

            if (!(obj instanceof DummySearchExtBuilder)) {
                return false;
            }

            return this.id.equals(((DummySearchExtBuilder) obj).getId());
        }

        public static DummySearchExtBuilder parse(XContentParser parser) throws IOException {
            String id;
            XContentParser.Token token = parser.currentToken();
            if (token == XContentParser.Token.VALUE_STRING) {
                id = parser.text();
            } else {
                throw new ParsingException(parser.getTokenLocation(), "Expected a VALUE_STRING but got " + token);
            }
            if (id == null) {
                throw new ParsingException(parser.getTokenLocation(), "no id specified for " + DUMMY_FIELD.getPreferredName());
            }
            return new DummySearchExtBuilder(id);
        }
    }

    static class FakeSearchExtBuilder extends DummySearchExtBuilder {
        static ParseField DUMMY_FIELD = new ParseField("fake");

        public FakeSearchExtBuilder(String id) {
            super(id);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(DUMMY_FIELD.getPreferredName());
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.field(DUMMY_FIELD.getPreferredName(), id);
        }
    }
}
