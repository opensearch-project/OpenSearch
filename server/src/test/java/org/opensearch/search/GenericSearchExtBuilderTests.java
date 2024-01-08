/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.search;

import org.opensearch.Version;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchResponseTests;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.rest.action.search.RestSearchAction;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.UUID;

import static java.util.Collections.singletonMap;

public class GenericSearchExtBuilderTests extends OpenSearchTestCase {

    private static final NamedXContentRegistry xContentRegistry;
    static {
        List<NamedXContentRegistry.Entry> namedXContents = new ArrayList<>(InternalAggregationTestCase.getDefaultNamedXContents());
        namedXContents.addAll(SuggestTests.getDefaultNamedXContents());
        namedXContents.add(
            new NamedXContentRegistry.Entry(
                SearchExtBuilder.class,
                GenericSearchExtBuilder.EXT_BUILDER_NAME,
                GenericSearchExtBuilder::fromXContent
            )
        );
        xContentRegistry = new NamedXContentRegistry(namedXContents);
    }

    private final NamedWriteableRegistry namedWriteableRegistry = new NamedWriteableRegistry(
        new SearchModule(Settings.EMPTY, List.of(new SearchPlugin() {
            @Override
            public List<SearchExtSpec<?>> getSearchExts() {
                return List.of(
                    new SearchExtSpec<>(
                        GenericSearchExtBuilder.EXT_BUILDER_NAME,
                        GenericSearchExtBuilder::new,
                        GenericSearchExtBuilder::fromXContent
                    )
                );
            }
        })).getNamedWriteables()
    );

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }

    SearchResponseTests srt = new SearchResponseTests();
    private AggregationsTests aggregationsTests = new AggregationsTests();

    @Before
    public void init() throws Exception {
        aggregationsTests.init();
    }

    @After
    public void cleanUp() throws Exception {
        aggregationsTests.cleanUp();
    }

    public void testFromXContentWithUnregisteredSearchExtBuilders() throws IOException {
        List<NamedXContentRegistry.Entry> namedXContents = new ArrayList<>(InternalAggregationTestCase.getDefaultNamedXContents());
        namedXContents.addAll(SuggestTests.getDefaultNamedXContents());
        String dummyId = UUID.randomUUID().toString();
        List<SearchExtBuilder> extBuilders = List.of(
            new SimpleValueSearchExtBuilder(dummyId),
            new MapSearchExtBuilder(Map.of("x", "y", "a", "b")),
            new ListSearchExtBuilder(List.of("1", "2", "3"))
        );
        SearchResponse response = srt.createTestItem(false, extBuilders);
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

    // This test case fails because GenericSearchExtBuilder does not retain the name of the SearchExtBuilder that it is replacing.
    // GenericSearchExtBuilder has its own "generic_ext" section name.
    // public void testFromXContentWithSearchExtBuilders() throws IOException {
    // String dummyId = UUID.randomUUID().toString();
    // srt.doFromXContentTestWithRandomFields(createTestItem(false, List.of(new SimpleValueSearchExtBuilder(dummyId))), false);
    // }

    public void testFromXContentWithGenericSearchExtBuildersForSimpleValues() throws IOException {
        String dummyId = UUID.randomUUID().toString();
        srt.doFromXContentTestWithRandomFields(
            createTestItem(false, List.of(new GenericSearchExtBuilder(dummyId, GenericSearchExtBuilder.ValueType.SIMPLE))),
            false
        );
    }

    public void testFromXContentWithGenericSearchExtBuildersForMapValues() throws IOException {
        srt.doFromXContentTestWithRandomFields(
            createTestItem(false, List.of(new GenericSearchExtBuilder(Map.of("x", "y", "a", "b"), GenericSearchExtBuilder.ValueType.MAP))),
            false
        );
    }

    public void testFromXContentWithGenericSearchExtBuildersForListValues() throws IOException {
        String dummyId = UUID.randomUUID().toString();
        srt.doFromXContentTestWithRandomFields(
            createTestItem(false, List.of(new GenericSearchExtBuilder(List.of("1", "2", "3"), GenericSearchExtBuilder.ValueType.LIST))),
            false
        );
    }

    public void testSerializationWithGenericSearchExtBuildersForSimpleValues() throws IOException {
        String id = UUID.randomUUID().toString();
        SearchResponse searchResponse = createTestItem(
            false,
            List.of(new GenericSearchExtBuilder(id, GenericSearchExtBuilder.ValueType.SIMPLE))
        );
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

    public void testSerializationWithGenericSearchExtBuildersForMapValues() throws IOException {
        SearchResponse searchResponse = createTestItem(
            false,
            List.of(new GenericSearchExtBuilder(Map.of("x", "y", "a", "b"), GenericSearchExtBuilder.ValueType.MAP))
        );
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

    public void testSerializationWithGenericSearchExtBuildersForListValues() throws IOException {
        SearchResponse searchResponse = createTestItem(
            false,
            List.of(new GenericSearchExtBuilder(List.of("1", "2", "3"), GenericSearchExtBuilder.ValueType.LIST))
        );
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

    static class SimpleValueSearchExtBuilder extends SearchExtBuilder {

        static ParseField FIELD = new ParseField("simple_value");

        private final String id;

        public SimpleValueSearchExtBuilder(String id) {
            assertNotNull(id);
            this.id = id;
        }

        public SimpleValueSearchExtBuilder(StreamInput in) throws IOException {
            this.id = in.readString();
        }

        public String getId() {
            return this.id;
        }

        @Override
        public String getWriteableName() {
            return FIELD.getPreferredName();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(this.id);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.field(FIELD.getPreferredName(), id);
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

            if (!(obj instanceof SimpleValueSearchExtBuilder)) {
                return false;
            }

            return this.id.equals(((SimpleValueSearchExtBuilder) obj).getId());
        }

        public static SimpleValueSearchExtBuilder parse(XContentParser parser) throws IOException {
            String id;
            XContentParser.Token token = parser.currentToken();
            if (token == XContentParser.Token.VALUE_STRING) {
                id = parser.text();
            } else {
                throw new ParsingException(parser.getTokenLocation(), "Expected a VALUE_STRING but got " + token);
            }
            if (id == null) {
                throw new ParsingException(parser.getTokenLocation(), "no id specified for " + FIELD.getPreferredName());
            }
            return new SimpleValueSearchExtBuilder(id);
        }
    }

    static class MapSearchExtBuilder extends SearchExtBuilder {

        private final static String EXT_FIELD = "map0";

        private final Map<String, Object> map;

        public MapSearchExtBuilder(Map<String, String> map) {
            this.map = new HashMap<>();
            for (Map.Entry<String, String> e : map.entrySet()) {
                this.map.put(e.getKey(), e.getValue());
            }
        }

        @Override
        public String getWriteableName() {
            return EXT_FIELD;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(this.map);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.field(EXT_FIELD, this.map);
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.getClass(), this.map);
        }

        @Override
        public boolean equals(Object obj) {
            return false;
        }
    }

    static class ListSearchExtBuilder extends SearchExtBuilder {

        private final static String EXT_FIELD = "list0";

        private final List<String> list;

        public ListSearchExtBuilder(List<String> list) {
            this.list = new ArrayList<>();
            list.forEach(e -> this.list.add(e));
        }

        @Override
        public String getWriteableName() {
            return EXT_FIELD;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeCollection(this.list, StreamOutput::writeString);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            return builder.field(EXT_FIELD, this.list);
        }

        @Override
        public int hashCode() {
            return Objects.hash(this.getClass(), this.list);
        }

        @Override
        public boolean equals(Object obj) {
            return false;
        }
    }
}
