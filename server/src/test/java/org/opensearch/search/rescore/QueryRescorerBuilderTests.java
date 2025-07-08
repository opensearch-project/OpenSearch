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

package org.opensearch.search.rescore;

import org.apache.lucene.search.Query;
import org.opensearch.OpenSearchParseException;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedObjectNotFoundException;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParseException;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.ContentPath;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.TextFieldMapper;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryRewriteContext;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.Rewriteable;
import org.opensearch.search.SearchModule;
import org.opensearch.search.rescore.QueryRescorer.QueryRescoreContext;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import java.io.IOException;

import static java.util.Collections.emptyList;
import static org.opensearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.hamcrest.Matchers.containsString;

public class QueryRescorerBuilderTests extends OpenSearchTestCase {

    private static final int NUMBER_OF_TESTBUILDERS = 20;
    private static NamedWriteableRegistry namedWriteableRegistry;
    private static NamedXContentRegistry xContentRegistry;

    /**
     * setup for the whole base test class
     */
    @BeforeClass
    public static void init() {
        SearchModule searchModule = new SearchModule(Settings.EMPTY, emptyList());
        namedWriteableRegistry = new NamedWriteableRegistry(searchModule.getNamedWriteables());
        xContentRegistry = new NamedXContentRegistry(searchModule.getNamedXContents());
    }

    @AfterClass
    public static void afterClass() throws Exception {
        namedWriteableRegistry = null;
        xContentRegistry = null;
    }

    /**
     * Test serialization and deserialization of the rescore builder
     */
    public void testSerialization() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            RescorerBuilder<?> original = randomRescoreBuilder();
            RescorerBuilder<?> deserialized = copy(original);
            assertEquals(deserialized, original);
            assertEquals(deserialized.hashCode(), original.hashCode());
            assertNotSame(deserialized, original);
        }
    }

    /**
     * Test equality and hashCode properties
     */
    public void testEqualsAndHashcode() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            checkEqualsAndHashCode(randomRescoreBuilder(), this::copy, QueryRescorerBuilderTests::mutate);
        }
    }

    private RescorerBuilder<?> copy(RescorerBuilder<?> original) throws IOException {
        return copyWriteable(
            original,
            namedWriteableRegistry,
            namedWriteableRegistry.getReader(RescorerBuilder.class, original.getWriteableName())
        );
    }

    /**
     *  creates random rescorer, renders it to xContent and back to new instance that should be equal to original
     */
    public void testFromXContent() throws IOException {
        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            RescorerBuilder<?> rescoreBuilder = randomRescoreBuilder();
            XContentBuilder builder = MediaTypeRegistry.contentBuilder(randomFrom(XContentType.values()));
            if (randomBoolean()) {
                builder.prettyPrint();
            }
            rescoreBuilder.toXContent(builder, ToXContent.EMPTY_PARAMS);
            XContentBuilder shuffled = shuffleXContent(builder);

            try (XContentParser parser = createParser(shuffled)) {
                parser.nextToken();
                RescorerBuilder<?> secondRescoreBuilder = RescorerBuilder.parseFromXContent(parser);
                assertNotSame(rescoreBuilder, secondRescoreBuilder);
                assertEquals(rescoreBuilder, secondRescoreBuilder);
                assertEquals(rescoreBuilder.hashCode(), secondRescoreBuilder.hashCode());
            }
        }
    }

    /**
     * test that build() outputs a {@link RescoreContext} that has the same properties
     * than the test builder
     */
    public void testBuildRescoreSearchContext() throws OpenSearchParseException, IOException {
        final long nowInMillis = randomNonNegativeLong();
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings(randomAlphaOfLengthBetween(1, 10), indexSettings);
        // shard context will only need indicesQueriesRegistry for building Query objects nested in query rescorer
        QueryShardContext mockShardContext = new QueryShardContext(
            0,
            idxSettings,
            BigArrays.NON_RECYCLING_INSTANCE,
            null,
            null,
            null,
            null,
            null,
            xContentRegistry(),
            namedWriteableRegistry,
            null,
            null,
            () -> nowInMillis,
            null,
            null,
            () -> true,
            null
        ) {
            @Override
            public MappedFieldType fieldMapper(String name) {
                TextFieldMapper.Builder builder = new TextFieldMapper.Builder(name, createDefaultIndexAnalyzers());
                return builder.build(new Mapper.BuilderContext(idxSettings.getSettings(), new ContentPath(1))).fieldType();
            }
        };

        for (int runs = 0; runs < NUMBER_OF_TESTBUILDERS; runs++) {
            QueryRescorerBuilder rescoreBuilder = randomRescoreBuilder();
            QueryRescoreContext rescoreContext = (QueryRescoreContext) rescoreBuilder.buildContext(mockShardContext);
            int expectedWindowSize = rescoreBuilder.windowSize() == null
                ? RescorerBuilder.DEFAULT_WINDOW_SIZE
                : rescoreBuilder.windowSize().intValue();
            assertEquals(expectedWindowSize, rescoreContext.getWindowSize());
            Query expectedQuery = Rewriteable.rewrite(rescoreBuilder.getRescoreQuery(), mockShardContext).toQuery(mockShardContext);
            assertEquals(expectedQuery, rescoreContext.parsedQuery().query());
            assertEquals(rescoreBuilder.getQueryWeight(), rescoreContext.queryWeight(), Float.MIN_VALUE);
            assertEquals(rescoreBuilder.getRescoreQueryWeight(), rescoreContext.rescoreQueryWeight(), Float.MIN_VALUE);
            assertEquals(rescoreBuilder.getScoreMode(), rescoreContext.scoreMode());
        }
    }

    public void testRescoreQueryNull() throws IOException {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> new QueryRescorerBuilder((QueryBuilder) null));
        assertEquals("rescore_query cannot be null", e.getMessage());
    }

    /**
     * Test that named queries from rescore contexts are captured
     */
    public void testRescoreNamedQueries() throws IOException {
        final long nowInMillis = randomNonNegativeLong();
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings(randomAlphaOfLengthBetween(1, 10), indexSettings);

        QueryShardContext mockShardContext = new QueryShardContext(
            0,
            idxSettings,
            BigArrays.NON_RECYCLING_INSTANCE,
            null,
            null,
            null,
            null,
            null,
            xContentRegistry(),
            namedWriteableRegistry,
            null,
            null,
            () -> nowInMillis,
            null,
            null,
            () -> true,
            null
        ) {
            @Override
            public MappedFieldType fieldMapper(String name) {
                TextFieldMapper.Builder builder = new TextFieldMapper.Builder(name, createDefaultIndexAnalyzers());
                return builder.build(new Mapper.BuilderContext(idxSettings.getSettings(), new ContentPath(1))).fieldType();
            }
        };

        QueryBuilder namedQueryBuilder = new MatchAllQueryBuilder().queryName("test_rescore_query");
        QueryRescorerBuilder rescoreBuilder = new QueryRescorerBuilder(namedQueryBuilder);
        QueryRescoreContext rescoreContext = (QueryRescoreContext) rescoreBuilder.buildContext(mockShardContext);
        assertNotNull(rescoreContext.parsedQuery());
        assertNotNull(rescoreContext.parsedQuery().namedFilters());
        assertEquals(1, rescoreContext.parsedQuery().namedFilters().size());
        assertTrue(rescoreContext.parsedQuery().namedFilters().containsKey("test_rescore_query"));
        assertNotNull(rescoreContext.parsedQuery().namedFilters().get("test_rescore_query"));
    }

    class AlwaysRewriteQueryBuilder extends MatchAllQueryBuilder {

        @Override
        protected QueryBuilder doRewrite(QueryRewriteContext queryShardContext) throws IOException {
            return new MatchAllQueryBuilder();
        }
    }

    public void testRewritingKeepsSettings() throws IOException {

        final long nowInMillis = randomNonNegativeLong();
        Settings indexSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();
        IndexSettings idxSettings = IndexSettingsModule.newIndexSettings(randomAlphaOfLengthBetween(1, 10), indexSettings);
        // shard context will only need indicesQueriesRegistry for building Query objects nested in query rescorer
        QueryShardContext mockShardContext = new QueryShardContext(
            0,
            idxSettings,
            BigArrays.NON_RECYCLING_INSTANCE,
            null,
            null,
            null,
            null,
            null,
            xContentRegistry(),
            namedWriteableRegistry,
            null,
            null,
            () -> nowInMillis,
            null,
            null,
            () -> true,
            null
        ) {
            @Override
            public MappedFieldType fieldMapper(String name) {
                TextFieldMapper.Builder builder = new TextFieldMapper.Builder(name, createDefaultIndexAnalyzers());
                return builder.build(new Mapper.BuilderContext(idxSettings.getSettings(), new ContentPath(1))).fieldType();
            }
        };

        QueryBuilder rewriteQb = new AlwaysRewriteQueryBuilder();
        org.opensearch.search.rescore.QueryRescorerBuilder rescoreBuilder = new org.opensearch.search.rescore.QueryRescorerBuilder(
            rewriteQb
        );

        rescoreBuilder.setQueryWeight(randomFloat());
        rescoreBuilder.setRescoreQueryWeight(randomFloat());
        rescoreBuilder.setScoreMode(QueryRescoreMode.Max);
        rescoreBuilder.windowSize(randomIntBetween(0, 100));

        QueryRescorerBuilder rescoreRewritten = rescoreBuilder.rewrite(mockShardContext);
        assertEquals(rescoreRewritten.getQueryWeight(), rescoreBuilder.getQueryWeight(), 0.01f);
        assertEquals(rescoreRewritten.getRescoreQueryWeight(), rescoreBuilder.getRescoreQueryWeight(), 0.01f);
        assertEquals(rescoreRewritten.getScoreMode(), rescoreBuilder.getScoreMode());
        assertEquals(rescoreRewritten.windowSize(), rescoreBuilder.windowSize());
    }

    /**
     * test parsing exceptions for incorrect rescorer syntax
     */
    public void testUnknownFieldsExpection() throws IOException {

        String rescoreElement = "{\n" + "    \"window_size\" : 20,\n" + "    \"bad_rescorer_name\" : { }\n" + "}\n";
        try (XContentParser parser = createParser(rescoreElement)) {
            Exception e = expectThrows(NamedObjectNotFoundException.class, () -> RescorerBuilder.parseFromXContent(parser));
            assertEquals("[3:27] unknown field [bad_rescorer_name]", e.getMessage());
        }
        rescoreElement = "{\n" + "    \"bad_fieldName\" : 20\n" + "}\n";
        try (XContentParser parser = createParser(rescoreElement)) {
            Exception e = expectThrows(ParsingException.class, () -> RescorerBuilder.parseFromXContent(parser));
            assertEquals("rescore doesn't support [bad_fieldName]", e.getMessage());
        }

        rescoreElement = "{\n" + "    \"window_size\" : 20,\n" + "    \"query\" : [ ]\n" + "}\n";
        try (XContentParser parser = createParser(rescoreElement)) {
            Exception e = expectThrows(ParsingException.class, () -> RescorerBuilder.parseFromXContent(parser));
            assertEquals("unexpected token [START_ARRAY] after [query]", e.getMessage());
        }

        rescoreElement = "{ }";
        try (XContentParser parser = createParser(rescoreElement)) {
            Exception e = expectThrows(ParsingException.class, () -> RescorerBuilder.parseFromXContent(parser));
            assertEquals("missing rescore type", e.getMessage());
        }

        rescoreElement = "{\n" + "    \"window_size\" : 20,\n" + "    \"query\" : { \"bad_fieldname\" : 1.0  } \n" + "}\n";
        try (XContentParser parser = createParser(rescoreElement)) {
            XContentParseException e = expectThrows(XContentParseException.class, () -> RescorerBuilder.parseFromXContent(parser));
            assertEquals("[3:17] [query] unknown field [bad_fieldname]", e.getMessage());
        }

        rescoreElement = "{\n"
            + "    \"window_size\" : 20,\n"
            + "    \"query\" : { \"rescore_query\" : { \"unknown_queryname\" : { } } } \n"
            + "}\n";
        try (XContentParser parser = createParser(rescoreElement)) {
            Exception e = expectThrows(XContentParseException.class, () -> RescorerBuilder.parseFromXContent(parser));
            assertThat(e.getMessage(), containsString("[query] failed to parse field [rescore_query]"));
        }

        rescoreElement = "{\n"
            + "    \"window_size\" : 20,\n"
            + "    \"query\" : { \"rescore_query\" : { \"match_all\" : { } } } \n"
            + "}\n";
        try (XContentParser parser = createParser(rescoreElement)) {
            RescorerBuilder.parseFromXContent(parser);
        }
    }

    /**
     * create a new parser from the rescorer string representation and reset context with it
     */
    private XContentParser createParser(String rescoreElement) throws IOException {
        XContentParser parser = createParser(JsonXContent.jsonXContent, rescoreElement);
        // move to first token, this is where the internal fromXContent
        assertTrue(parser.nextToken() == XContentParser.Token.START_OBJECT);
        return parser;
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return xContentRegistry;
    }

    private static RescorerBuilder<?> mutate(RescorerBuilder<?> original) throws IOException {
        RescorerBuilder<?> mutation = OpenSearchTestCase.copyWriteable(original, namedWriteableRegistry, QueryRescorerBuilder::new);
        if (randomBoolean()) {
            Integer windowSize = original.windowSize();
            if (windowSize != null) {
                mutation.windowSize(windowSize + 1);
            } else {
                mutation.windowSize(randomIntBetween(0, 100));
            }
        } else {
            QueryRescorerBuilder queryRescorer = (QueryRescorerBuilder) mutation;
            switch (randomIntBetween(0, 3)) {
                case 0:
                    queryRescorer.setQueryWeight(queryRescorer.getQueryWeight() + 0.1f);
                    break;
                case 1:
                    queryRescorer.setRescoreQueryWeight(queryRescorer.getRescoreQueryWeight() + 0.1f);
                    break;
                case 2:
                    QueryRescoreMode other;
                    do {
                        other = randomFrom(QueryRescoreMode.values());
                    } while (other == queryRescorer.getScoreMode());
                    queryRescorer.setScoreMode(other);
                    break;
                case 3:
                    // only increase the boost to make it a slightly different query
                    queryRescorer.getRescoreQuery().boost(queryRescorer.getRescoreQuery().boost() + 0.1f);
                    break;
                default:
                    throw new IllegalStateException("unexpected random mutation in test");
            }
        }
        return mutation;
    }

    /**
     * create random shape that is put under test
     */
    public static QueryRescorerBuilder randomRescoreBuilder() {
        QueryBuilder queryBuilder = new MatchAllQueryBuilder().boost(randomFloat()).queryName(randomAlphaOfLength(20));
        org.opensearch.search.rescore.QueryRescorerBuilder rescorer = new org.opensearch.search.rescore.QueryRescorerBuilder(queryBuilder);
        if (randomBoolean()) {
            rescorer.setQueryWeight(randomFloat());
        }
        if (randomBoolean()) {
            rescorer.setRescoreQueryWeight(randomFloat());
        }
        if (randomBoolean()) {
            rescorer.setScoreMode(randomFrom(QueryRescoreMode.values()));
        }
        if (randomBoolean()) {
            rescorer.windowSize(randomIntBetween(0, 100));
        }
        return rescorer;
    }
}
