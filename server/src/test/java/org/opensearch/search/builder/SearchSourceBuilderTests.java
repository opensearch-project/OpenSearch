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

package org.opensearch.search.builder;

import com.fasterxml.jackson.core.JsonParseException;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.RandomQueryBuilder;
import org.opensearch.index.query.Rewriteable;
import org.opensearch.script.Script;
import org.opensearch.search.AbstractSearchTestCase;
import org.opensearch.search.rescore.QueryRescorerBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.ScoreSortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.EqualsHashCodeTestUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Map;

import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasToString;

public class SearchSourceBuilderTests extends AbstractSearchTestCase {

    public void testFromXContent() throws IOException {
        SearchSourceBuilder testSearchSourceBuilder = createSearchSourceBuilder();
        XContentBuilder builder = MediaTypeRegistry.contentBuilder(randomFrom(XContentType.values()));
        if (randomBoolean()) {
            builder.prettyPrint();
        }
        testSearchSourceBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
        try (XContentParser xParser = createParser(builder)) {
            assertParseSearchSource(testSearchSourceBuilder, xParser);
        }
    }

    public void testFromXContentInvalid() throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{}}")) {
            JsonParseException exc = expectThrows(JsonParseException.class, () -> SearchSourceBuilder.fromXContent(parser));
            assertThat(exc.getMessage(), containsString("Unexpected close marker"));
        }

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, "{}{}")) {
            ParsingException exc = expectThrows(ParsingException.class, () -> SearchSourceBuilder.fromXContent(parser));
            assertThat(exc.getDetailedMessage(), containsString("found after the main object"));
        }
    }

    private static void assertParseSearchSource(SearchSourceBuilder testBuilder, XContentParser parser) throws IOException {
        if (randomBoolean()) {
            parser.nextToken(); // sometimes we move it on the START_OBJECT to
                                // test the embedded case
        }
        SearchSourceBuilder newBuilder = SearchSourceBuilder.fromXContent(parser);
        assertNull(parser.nextToken());
        assertEquals(testBuilder, newBuilder);
        assertEquals(testBuilder.hashCode(), newBuilder.hashCode());
    }

    public void testSerialization() throws IOException {
        SearchSourceBuilder testBuilder = createSearchSourceBuilder();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            testBuilder.writeTo(output);
            try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                SearchSourceBuilder deserializedBuilder = new SearchSourceBuilder(in);
                assertEquals(deserializedBuilder, testBuilder);
                assertEquals(deserializedBuilder.hashCode(), testBuilder.hashCode());
                assertNotSame(deserializedBuilder, testBuilder);
            }
        }
    }

    public void testSerializationWithPercentilesQueryObject() throws IOException {
        String restContent = "{\n"
            + "    \"aggregations\": {"
            + "        \"percentiles_duration\": {\n"
            + "            \"percentiles\" : {\n"
            + "                \"field\": \"duration\"\n"
            + "            }\n"
            + "        }\n"
            + "    }\n"
            + "}\n";
        String expectedContent = "{\"aggregations\":{"
            + "\"percentiles_duration\":{"
            + "\"percentiles\":{"
            + "\"field\":\"duration\","
            + "\"percents\":[1.0,5.0,25.0,50.0,75.0,95.0,99.0],"
            + "\"keyed\":true,"
            + "\"tdigest\":{"
            + "\"compression\":100.0"
            + "}"
            + "}"
            + "}"
            + "}}";

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
            SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(parser);

            try (BytesStreamOutput output = new BytesStreamOutput()) {
                searchSourceBuilder.writeTo(output);
                try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                    SearchSourceBuilder deserializedBuilder = new SearchSourceBuilder(in);
                    String actualContent = deserializedBuilder.toString();

                    assertEquals(expectedContent, actualContent);
                    assertEquals(searchSourceBuilder.hashCode(), deserializedBuilder.hashCode());
                    assertNotSame(searchSourceBuilder, deserializedBuilder);
                }
            }
        }
    }

    public void testShallowCopy() {
        for (int i = 0; i < 10; i++) {
            SearchSourceBuilder original = createSearchSourceBuilder();
            SearchSourceBuilder copy = original.shallowCopy();
            assertEquals(original, copy);
        }
    }

    public void testEqualsAndHashcode() throws IOException {
        // TODO add test checking that changing any member of this class produces an object that is not equal to the original
        EqualsHashCodeTestUtils.checkEqualsAndHashCode(createSearchSourceBuilder(), this::copyBuilder);
    }

    // we use the streaming infra to create a copy of the builder provided as argument
    private SearchSourceBuilder copyBuilder(SearchSourceBuilder original) throws IOException {
        return OpenSearchTestCase.copyWriteable(original, namedWriteableRegistry, SearchSourceBuilder::new);
    }

    public void testParseIncludeExclude() throws IOException {
        {
            String restContent = " { \"_source\": { \"includes\": \"include\", \"excludes\": \"*.field2\"}}";
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
                SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(parser);
                assertArrayEquals(new String[] { "*.field2" }, searchSourceBuilder.fetchSource().excludes());
                assertArrayEquals(new String[] { "include" }, searchSourceBuilder.fetchSource().includes());
            }
        }
        {
            String restContent = " { \"_source\": false}";
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
                SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(parser);
                assertArrayEquals(new String[] {}, searchSourceBuilder.fetchSource().excludes());
                assertArrayEquals(new String[] {}, searchSourceBuilder.fetchSource().includes());
                assertFalse(searchSourceBuilder.fetchSource().fetchSource());
            }
        }
    }

    public void testMultipleQueryObjectsAreRejected() throws Exception {
        String restContent = " { \"query\": {\n"
            + "    \"multi_match\": {\n"
            + "      \"query\": \"workd\",\n"
            + "      \"fields\": [\"title^5\", \"plain_body\"]\n"
            + "    },\n"
            + "    \"filters\": {\n"
            + "      \"terms\": {\n"
            + "        \"status\": [ 3 ]\n"
            + "      }\n"
            + "    }\n"
            + "  } }";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
            ParsingException e = expectThrows(ParsingException.class, () -> SearchSourceBuilder.fromXContent(parser));
            assertEquals("[multi_match] malformed query, expected [END_OBJECT] but found [FIELD_NAME]", e.getMessage());
        }
    }

    public void testParseAndRewrite() throws IOException {
        String restContent = "{\n"
            + "  \"query\": {\n"
            + "    \"bool\": {\n"
            + "      \"must\": {\n"
            + "        \"match_none\": {}\n"
            + "      }\n"
            + "    }\n"
            + "  },\n"
            + "  \"rescore\": {\n"
            + "    \"window_size\": 50,\n"
            + "    \"query\": {\n"
            + "      \"rescore_query\": {\n"
            + "        \"bool\": {\n"
            + "          \"must\": {\n"
            + "            \"match_none\": {}\n"
            + "          }\n"
            + "        }\n"
            + "      },\n"
            + "      \"rescore_query_weight\": 10\n"
            + "    }\n"
            + "  },\n"
            + "  \"highlight\": {\n"
            + "    \"order\": \"score\",\n"
            + "    \"fields\": {\n"
            + "      \"content\": {\n"
            + "        \"fragment_size\": 150,\n"
            + "        \"number_of_fragments\": 3,\n"
            + "        \"highlight_query\": {\n"
            + "          \"bool\": {\n"
            + "            \"must\": {\n"
            + "              \"match_none\": {}\n"
            + "            }\n"
            + "          }\n"
            + "        }\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
            SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(parser);
            assertThat(searchSourceBuilder.query(), instanceOf(BoolQueryBuilder.class));
            assertThat(searchSourceBuilder.rescores().get(0), instanceOf(QueryRescorerBuilder.class));
            assertThat(
                ((QueryRescorerBuilder) searchSourceBuilder.rescores().get(0)).getRescoreQuery(),
                instanceOf(BoolQueryBuilder.class)
            );
            assertThat(searchSourceBuilder.highlighter().fields().get(0).highlightQuery(), instanceOf(BoolQueryBuilder.class));
            searchSourceBuilder = rewrite(searchSourceBuilder);

            assertThat(searchSourceBuilder.query(), instanceOf(MatchNoneQueryBuilder.class));
            assertThat(searchSourceBuilder.rescores().get(0), instanceOf(QueryRescorerBuilder.class));
            assertThat(
                ((QueryRescorerBuilder) searchSourceBuilder.rescores().get(0)).getRescoreQuery(),
                instanceOf(MatchNoneQueryBuilder.class)
            );
            assertThat(searchSourceBuilder.highlighter().fields().get(0).highlightQuery(), instanceOf(MatchNoneQueryBuilder.class));
            assertEquals(searchSourceBuilder.highlighter().fields().get(0).fragmentSize().intValue(), 150);
            assertEquals(searchSourceBuilder.highlighter().fields().get(0).numOfFragments().intValue(), 3);

        }

    }

    public void testParseSort() throws IOException {
        {
            String restContent = " { \"sort\": \"foo\"}";
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
                SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(parser);
                searchSourceBuilder = rewrite(searchSourceBuilder);
                assertEquals(1, searchSourceBuilder.sorts().size());
                assertEquals(new FieldSortBuilder("foo"), searchSourceBuilder.sorts().get(0));
            }
        }

        {
            String restContent = "{\"sort\" : [\n"
                + "        { \"post_date\" : {\"order\" : \"asc\"}},\n"
                + "        \"user\",\n"
                + "        { \"name\" : \"desc\" },\n"
                + "        { \"age\" : \"desc\" },\n"
                + "        \"_score\"\n"
                + "    ]}";
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
                SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(parser);
                searchSourceBuilder = rewrite(searchSourceBuilder);
                assertEquals(5, searchSourceBuilder.sorts().size());
                assertEquals(new FieldSortBuilder("post_date"), searchSourceBuilder.sorts().get(0));
                assertEquals(new FieldSortBuilder("user"), searchSourceBuilder.sorts().get(1));
                assertEquals(new FieldSortBuilder("name").order(SortOrder.DESC), searchSourceBuilder.sorts().get(2));
                assertEquals(new FieldSortBuilder("age").order(SortOrder.DESC), searchSourceBuilder.sorts().get(3));
                assertEquals(new ScoreSortBuilder(), searchSourceBuilder.sorts().get(4));
            }
        }
    }

    public void testDerivedFieldsParsingAndSerialization() throws IOException {
        {
            String restContent = "{\n"
                + "  \"derived\": {\n"
                + "    \"duration\": {\n"
                + "      \"type\": \"long\",\n"
                + "      \"script\": \"emit(doc['test'])\"\n"
                + "    },\n"
                + "    \"ip_from_message\": {\n"
                + "      \"type\": \"keyword\",\n"
                + "      \"script\": \"emit(doc['message'])\"\n"
                + "    }\n"
                + "  },\n"
                + "    \"query\" : {\n"
                + "        \"match\": { \"content\": { \"query\": \"foo bar\" }}\n"
                + "     }\n"
                + "}";

            String expectedContent =
                "{\"query\":{\"match\":{\"content\":{\"query\":\"foo bar\",\"operator\":\"OR\",\"prefix_length\":0,\"max_expansions\":50,\"fuzzy_transpositions\":true,\"lenient\":false,\"zero_terms_query\":\"NONE\",\"auto_generate_synonyms_phrase_query\":true,\"boost\":1.0}}},"
                    + "\"derived\":{"
                    + "\"duration\":{\"type\":\"long\",\"script\":\"emit(doc['test'])\"},\"ip_from_message\":{\"type\":\"keyword\",\"script\":\"emit(doc['message'])\"},\"derived_field\":{\"type\":\"keyword\",\"script\":{\"source\":\"emit(doc['message']\",\"lang\":\"painless\"}}}}";

            try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
                SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(parser);
                searchSourceBuilder.derivedField("derived_field", "keyword", new Script("emit(doc['message']"));
                searchSourceBuilder = rewrite(searchSourceBuilder);
                assertEquals(2, searchSourceBuilder.getDerivedFieldsObject().size());
                assertEquals(1, searchSourceBuilder.getDerivedFields().size());

                try (BytesStreamOutput output = new BytesStreamOutput()) {
                    searchSourceBuilder.writeTo(output);
                    try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                        SearchSourceBuilder deserializedBuilder = new SearchSourceBuilder(in);
                        String actualContent = deserializedBuilder.toString();
                        assertEquals(expectedContent, actualContent);
                        assertEquals(searchSourceBuilder.hashCode(), deserializedBuilder.hashCode());
                        assertNotSame(searchSourceBuilder, deserializedBuilder);
                    }
                }
            }
        }

    }

    public void testDerivedFieldsParsingAndSerializationObjectType() throws IOException {
        {
            String restContent = "{\n"
                + "  \"derived\": {\n"
                + "    \"duration\": {\n"
                + "      \"type\": \"long\",\n"
                + "      \"script\": \"emit(doc['test'])\"\n"
                + "    },\n"
                + "    \"ip_from_message\": {\n"
                + "      \"type\": \"keyword\",\n"
                + "      \"script\": \"emit(doc['message'])\"\n"
                + "    },\n"
                + "    \"object\": {\n"
                + "      \"type\": \"object\",\n"
                + "      \"script\": \"emit(doc['test'])\",\n"
                + "      \"format\": \"dd-MM-yyyy\",\n"
                + "      \"prefilter_field\": \"test\",\n"
                + "      \"ignore_malformed\": true,\n"
                + "      \"properties\": {\n"
                + "         \"sub_field\": \"text\"\n"
                + "       }\n"
                + "    }\n"
                + "  },\n"
                + "    \"query\" : {\n"
                + "        \"match\": { \"content\": { \"query\": \"foo bar\" }}\n"
                + "     }\n"
                + "}";

            String expectedContent =
                "{\"query\":{\"match\":{\"content\":{\"query\":\"foo bar\",\"operator\":\"OR\",\"prefix_length\":0,\"max_expansions\":50,\"fuzzy_transpositions\":true,\"lenient\":false,\"zero_terms_query\":\"NONE\",\"auto_generate_synonyms_phrase_query\":true,\"boost\":1.0}}},\"derived\":{\"duration\":{\"type\":\"long\",\"script\":\"emit(doc['test'])\"},\"ip_from_message\":{\"type\":\"keyword\",\"script\":\"emit(doc['message'])\"},\"object\":{\"format\":\"dd-MM-yyyy\",\"prefilter_field\":\"test\",\"ignore_malformed\":true,\"type\":\"object\",\"script\":\"emit(doc['test'])\",\"properties\":{\"sub_field\":\"text\"}},\"derived_field\":{\"type\":\"object\",\"script\":{\"source\":\"emit(doc['message']\",\"lang\":\"painless\"},\"properties\":{\"sub_field_2\":\"keyword\"},\"prefilter_field\":\"message\",\"format\":\"dd-MM-yyyy\",\"ignore_malformed\":true}}}";

            try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
                SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(parser);
                searchSourceBuilder.derivedField(
                    "derived_field",
                    "object",
                    new Script("emit(doc['message']"),
                    Map.of("sub_field_2", "keyword"),
                    "message",
                    "dd-MM-yyyy",
                    true
                );
                searchSourceBuilder = rewrite(searchSourceBuilder);
                assertEquals(3, searchSourceBuilder.getDerivedFieldsObject().size());
                assertEquals(1, searchSourceBuilder.getDerivedFields().size());
                assertEquals(1, searchSourceBuilder.getDerivedFields().get(0).getProperties().size());
                assertEquals("message", searchSourceBuilder.getDerivedFields().get(0).getPrefilterField());
                assertEquals("dd-MM-yyyy", searchSourceBuilder.getDerivedFields().get(0).getFormat());
                assertTrue(searchSourceBuilder.getDerivedFields().get(0).getIgnoreMalformed());

                try (BytesStreamOutput output = new BytesStreamOutput()) {
                    searchSourceBuilder.writeTo(output);
                    try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                        SearchSourceBuilder deserializedBuilder = new SearchSourceBuilder(in);
                        String actualContent = deserializedBuilder.toString();
                        assertEquals(expectedContent, actualContent);
                        assertEquals(searchSourceBuilder.hashCode(), deserializedBuilder.hashCode());
                        assertNotSame(searchSourceBuilder, deserializedBuilder);
                    }
                }
            }
        }
    }

    public void testSearchPipelineParsingAndSerialization() throws IOException {
        String restContent = "{ \"query\": { \"match_all\": {} }, \"from\": 0, \"size\": 10, \"search_pipeline\": \"my_pipeline\" }";
        String expectedContent = "{\"from\":0,\"size\":10,\"query\":{\"match_all\":{\"boost\":1.0}},\"search_pipeline\":\"my_pipeline\"}";

        try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
            SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(parser);
            searchSourceBuilder = rewrite(searchSourceBuilder);

            try (BytesStreamOutput output = new BytesStreamOutput()) {
                searchSourceBuilder.writeTo(output);
                try (StreamInput in = new NamedWriteableAwareStreamInput(output.bytes().streamInput(), namedWriteableRegistry)) {
                    SearchSourceBuilder deserializedBuilder = new SearchSourceBuilder(in);
                    String actualContent = deserializedBuilder.toString();
                    assertEquals(expectedContent, actualContent);
                    assertEquals(searchSourceBuilder.hashCode(), deserializedBuilder.hashCode());
                    assertNotSame(searchSourceBuilder, deserializedBuilder);
                }
            }
        }
    }

    public void testAggsParsing() throws IOException {
        {
            String restContent = "{\n"
                + "    "
                + "\"aggs\": {"
                + "        \"test_agg\": {\n"
                + "            "
                + "\"terms\" : {\n"
                + "                \"field\": \"foo\"\n"
                + "            }\n"
                + "        }\n"
                + "    }\n"
                + "}\n";
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
                SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(parser);
                searchSourceBuilder = rewrite(searchSourceBuilder);
                assertEquals(1, searchSourceBuilder.aggregations().count());
            }
        }
        {
            String restContent = "{\n"
                + "    \"aggregations\": {"
                + "        \"test_agg\": {\n"
                + "            \"terms\" : {\n"
                + "                \"field\": \"foo\"\n"
                + "            }\n"
                + "        }\n"
                + "    }\n"
                + "}\n";
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
                SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(parser);
                searchSourceBuilder = rewrite(searchSourceBuilder);
                assertEquals(1, searchSourceBuilder.aggregations().count());
            }
        }
    }

    /**
     * test that we can parse the `rescore` element either as single object or as array
     */
    public void testParseRescore() throws IOException {
        {
            String restContent = "{\n"
                + "    \"query\" : {\n"
                + "        \"match\": { \"content\": { \"query\": \"foo bar\" }}\n"
                + "     },\n"
                + "    \"rescore\": {"
                + "        \"window_size\": 50,\n"
                + "        \"query\": {\n"
                + "            \"rescore_query\" : {\n"
                + "                \"match\": { \"content\": { \"query\": \"baz\" } }\n"
                + "            }\n"
                + "        }\n"
                + "    }\n"
                + "}\n";
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
                SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(parser);
                searchSourceBuilder = rewrite(searchSourceBuilder);
                assertEquals(1, searchSourceBuilder.rescores().size());
                assertEquals(
                    new QueryRescorerBuilder(QueryBuilders.matchQuery("content", "baz")).windowSize(50),
                    searchSourceBuilder.rescores().get(0)
                );
            }
        }

        {
            String restContent = "{\n"
                + "    \"query\" : {\n"
                + "        \"match\": { \"content\": { \"query\": \"foo bar\" }}\n"
                + "     },\n"
                + "    \"rescore\": [ {"
                + "        \"window_size\": 50,\n"
                + "        \"query\": {\n"
                + "            \"rescore_query\" : {\n"
                + "                \"match\": { \"content\": { \"query\": \"baz\" } }\n"
                + "            }\n"
                + "        }\n"
                + "    } ]\n"
                + "}\n";
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
                SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(parser);
                searchSourceBuilder = rewrite(searchSourceBuilder);
                assertEquals(1, searchSourceBuilder.rescores().size());
                assertEquals(
                    new QueryRescorerBuilder(QueryBuilders.matchQuery("content", "baz")).windowSize(50),
                    searchSourceBuilder.rescores().get(0)
                );
            }
        }
    }

    public void testTimeoutWithUnits() throws IOException {
        final String timeout = randomTimeValue();
        final String query = "{ \"query\": { \"match_all\": {}}, \"timeout\": \"" + timeout + "\"}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, query)) {
            final SearchSourceBuilder builder = SearchSourceBuilder.fromXContent(parser);
            assertThat(builder.timeout(), equalTo(TimeValue.parseTimeValue(timeout, null, "timeout")));
        }
    }

    public void testTimeoutWithoutUnits() throws IOException {
        final int timeout = randomIntBetween(1, 1024);
        final String query = "{ \"query\": { \"match_all\": {}}, \"timeout\": \"" + timeout + "\"}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, query)) {
            final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> SearchSourceBuilder.fromXContent(parser));
            assertThat(e, hasToString(containsString("unit is missing or unrecognized")));
        }
    }

    public void testToXContent() throws IOException {
        // verify that only what is set gets printed out through toXContent
        XContentType xContentType = randomFrom(XContentType.values());
        {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            XContentBuilder builder = MediaTypeRegistry.contentBuilder(xContentType);
            searchSourceBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
            BytesReference source = BytesReference.bytes(builder);
            Map<String, Object> sourceAsMap = XContentHelper.convertToMap(source, false, xContentType).v2();
            assertEquals(0, sourceAsMap.size());
        }
        {
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.query(RandomQueryBuilder.createQuery(random()));
            XContentBuilder builder = MediaTypeRegistry.contentBuilder(xContentType);
            searchSourceBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
            BytesReference source = BytesReference.bytes(builder);
            Map<String, Object> sourceAsMap = XContentHelper.convertToMap(source, false, xContentType).v2();
            assertEquals(1, sourceAsMap.size());
            assertEquals("query", sourceAsMap.keySet().iterator().next());
        }
    }

    public void testToXContentWithPointInTime() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TimeValue keepAlive = randomBoolean() ? TimeValue.timeValueHours(1) : null;
        searchSourceBuilder.pointInTimeBuilder(new PointInTimeBuilder("id").setKeepAlive(keepAlive));
        XContentBuilder builder = MediaTypeRegistry.contentBuilder(xContentType);
        searchSourceBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
        BytesReference bytes = BytesReference.bytes(builder);
        Map<String, Object> sourceAsMap = XContentHelper.convertToMap(bytes, false, xContentType).v2();
        assertEquals(1, sourceAsMap.size());
        @SuppressWarnings("unchecked")
        Map<String, Object> pit = (Map<String, Object>) sourceAsMap.get("pit");
        assertEquals("id", pit.get("id"));
        if (keepAlive != null) {
            assertEquals("1h", pit.get("keep_alive"));
            assertEquals(2, pit.size());
        } else {
            assertNull(pit.get("keep_alive"));
            assertEquals(1, pit.size());
        }
    }

    public void testParseIndicesBoost() throws IOException {
        {
            String restContent = " { \"indices_boost\": {\"foo\": 1.0, \"bar\": 2.0}}";
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
                SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(parser);
                assertEquals(2, searchSourceBuilder.indexBoosts().size());
                assertEquals(new SearchSourceBuilder.IndexBoost("foo", 1.0f), searchSourceBuilder.indexBoosts().get(0));
                assertEquals(new SearchSourceBuilder.IndexBoost("bar", 2.0f), searchSourceBuilder.indexBoosts().get(1));
                assertWarnings("Object format in indices_boost is deprecated, please use array format instead");
            }
        }

        {
            String restContent = "{"
                + "    \"indices_boost\" : [\n"
                + "        { \"foo\" : 1.0 },\n"
                + "        { \"bar\" : 2.0 },\n"
                + "        { \"baz\" : 3.0 }\n"
                + "    ]}";
            try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
                SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(parser);
                assertEquals(3, searchSourceBuilder.indexBoosts().size());
                assertEquals(new SearchSourceBuilder.IndexBoost("foo", 1.0f), searchSourceBuilder.indexBoosts().get(0));
                assertEquals(new SearchSourceBuilder.IndexBoost("bar", 2.0f), searchSourceBuilder.indexBoosts().get(1));
                assertEquals(new SearchSourceBuilder.IndexBoost("baz", 3.0f), searchSourceBuilder.indexBoosts().get(2));
            }
        }

        {
            String restContent = "{" + "    \"indices_boost\" : [\n" + "        { \"foo\" : 1.0, \"bar\": 2.0}\n" + // invalid format
                "    ]}";

            assertIndicesBoostParseErrorMessage(restContent, "Expected [END_OBJECT] in [indices_boost] but found [FIELD_NAME]");
        }

        {
            String restContent = "{" + "    \"indices_boost\" : [\n" + "        {}\n" + // invalid format
                "    ]}";

            assertIndicesBoostParseErrorMessage(restContent, "Expected [FIELD_NAME] in [indices_boost] but found [END_OBJECT]");
        }

        {
            String restContent = "{" + "    \"indices_boost\" : [\n" + "        { \"foo\" : \"bar\"}\n" + // invalid format
                "    ]}";

            assertIndicesBoostParseErrorMessage(restContent, "Expected [VALUE_NUMBER] in [indices_boost] but found [VALUE_STRING]");
        }

        {
            String restContent = "{" + "    \"indices_boost\" : [\n" + "        { \"foo\" : {\"bar\": 1}}\n" + // invalid format
                "    ]}";

            assertIndicesBoostParseErrorMessage(restContent, "Expected [VALUE_NUMBER] in [indices_boost] but found [START_OBJECT]");
        }
    }

    public void testNegativeFromErrors() {
        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> new SearchSourceBuilder().from(-2));
        assertEquals("[from] parameter cannot be negative, found [-2]", expected.getMessage());
    }

    public void testNegativeSizeErrors() {
        int randomSize = randomIntBetween(-100000, -2);
        IllegalArgumentException expected = expectThrows(IllegalArgumentException.class, () -> new SearchSourceBuilder().size(randomSize));
        assertEquals("[size] parameter cannot be negative, found [" + randomSize + "]", expected.getMessage());
        expected = expectThrows(IllegalArgumentException.class, () -> new SearchSourceBuilder().size(-1));
        assertEquals("[size] parameter cannot be negative, found [-1]", expected.getMessage());
    }

    public void testParseFromAndSize() throws IOException {
        int negativeFrom = randomIntBetween(-100, -1);
        String restContent = " { \"from\": \"" + negativeFrom + "\"}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
            IllegalArgumentException expected = expectThrows(
                IllegalArgumentException.class,
                () -> SearchSourceBuilder.fromXContent(parser)
            );
            assertEquals("[from] parameter cannot be negative, found [" + negativeFrom + "]", expected.getMessage());
        }

        int validFrom = randomIntBetween(0, 100);
        restContent = " { \"from\": \"" + validFrom + "\"}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
            SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(parser);
            assertEquals(validFrom, searchSourceBuilder.from());
        }

        int negativeSize = randomIntBetween(-100, -1);
        restContent = " { \"size\": \"" + negativeSize + "\"}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
            IllegalArgumentException expected = expectThrows(
                IllegalArgumentException.class,
                () -> SearchSourceBuilder.fromXContent(parser)
            );
            assertEquals("[size] parameter cannot be negative, found [" + negativeSize + "]", expected.getMessage());
        }

        int validSize = randomIntBetween(0, 100);
        restContent = " { \"size\": \"" + validSize + "\"}";
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
            SearchSourceBuilder searchSourceBuilder = SearchSourceBuilder.fromXContent(parser);
            assertEquals(validSize, searchSourceBuilder.size());
        }
    }

    private void assertIndicesBoostParseErrorMessage(String restContent, String expectedErrorMessage) throws IOException {
        try (XContentParser parser = createParser(JsonXContent.jsonXContent, restContent)) {
            ParsingException e = expectThrows(ParsingException.class, () -> SearchSourceBuilder.fromXContent(parser));
            assertEquals(expectedErrorMessage, e.getMessage());
        }
    }

    private SearchSourceBuilder rewrite(SearchSourceBuilder searchSourceBuilder) throws IOException {
        return Rewriteable.rewrite(
            searchSourceBuilder,
            new QueryRewriteContext(xContentRegistry(), writableRegistry(), null, Long.valueOf(1)::longValue)
        );
    }
}
