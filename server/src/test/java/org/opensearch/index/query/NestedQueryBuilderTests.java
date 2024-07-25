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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.index.query;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.join.ScoreMode;
import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.index.search.OpenSearchToParentBlockJoinQuery;
import org.opensearch.search.fetch.subphase.InnerHitsContext;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.AbstractQueryTestCase;
import org.opensearch.test.VersionUtils;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.opensearch.index.IndexSettingsTests.newIndexMeta;
import static org.opensearch.index.query.InnerHitBuilderTests.randomNestedInnerHits;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class NestedQueryBuilderTests extends AbstractQueryTestCase<NestedQueryBuilder> {

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        mapperService.merge(
            "_doc",
            new CompressedXContent(
                PutMappingRequest.simpleMapping(
                    TEXT_FIELD_NAME,
                    "type=text",
                    INT_FIELD_NAME,
                    "type=integer",
                    DOUBLE_FIELD_NAME,
                    "type=double",
                    BOOLEAN_FIELD_NAME,
                    "type=boolean",
                    DATE_FIELD_NAME,
                    "type=date",
                    OBJECT_FIELD_NAME,
                    "type=object",
                    GEO_POINT_FIELD_NAME,
                    "type=geo_point",
                    "nested1",
                    "type=nested"
                ).toString()
            ),
            MapperService.MergeReason.MAPPING_UPDATE
        );
    }

    /**
     * @return a {@link NestedQueryBuilder} with random values all over the place
     */
    @Override
    protected NestedQueryBuilder doCreateTestQueryBuilder() {
        QueryBuilder innerQueryBuilder = RandomQueryBuilder.createQuery(random());
        NestedQueryBuilder nqb = new NestedQueryBuilder("nested1", innerQueryBuilder, RandomPicks.randomFrom(random(), ScoreMode.values()));
        nqb.ignoreUnmapped(randomBoolean());
        if (randomBoolean()) {
            nqb.innerHit(
                new InnerHitBuilder(randomAlphaOfLengthBetween(1, 10)).setSize(randomIntBetween(0, 100))
                    .addSort(new FieldSortBuilder(INT_FIELD_NAME).order(SortOrder.ASC))
                    .setIgnoreUnmapped(nqb.ignoreUnmapped())
            );
        }
        return nqb;
    }

    @Override
    protected void doAssertLuceneQuery(NestedQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        assertThat(query, instanceOf(OpenSearchToParentBlockJoinQuery.class));
        // TODO how to assert this?
        if (queryBuilder.innerHit() != null) {
            // have to rewrite again because the provided queryBuilder hasn't been rewritten (directly returned from
            // doCreateTestQueryBuilder)
            queryBuilder = (NestedQueryBuilder) queryBuilder.rewrite(context);

            assertNotNull(context);
            Map<String, InnerHitContextBuilder> innerHitInternals = new HashMap<>();
            InnerHitContextBuilder.extractInnerHits(queryBuilder, innerHitInternals);
            assertTrue(innerHitInternals.containsKey(queryBuilder.innerHit().getName()));
            InnerHitContextBuilder innerHits = innerHitInternals.get(queryBuilder.innerHit().getName());
            assertEquals(innerHits.innerHitBuilder(), queryBuilder.innerHit());
        }
    }

    /**
     * Test (de)serialization on all previous released versions
     */
    public void testSerializationBWC() throws IOException {
        for (Version version : VersionUtils.allReleasedVersions()) {
            NestedQueryBuilder testQuery = createTestQueryBuilder();
            assertSerialization(testQuery, version);
        }
    }

    public void testPath() {
        assertEquals("nested1", createTestQueryBuilder().path());
    }

    public void testValidate() {
        QueryBuilder innerQuery = RandomQueryBuilder.createQuery(random());
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> QueryBuilders.nestedQuery(null, innerQuery, ScoreMode.Avg)
        );
        assertThat(e.getMessage(), equalTo("[nested] requires 'path' field"));

        e = expectThrows(IllegalArgumentException.class, () -> QueryBuilders.nestedQuery("foo", null, ScoreMode.Avg));
        assertThat(e.getMessage(), equalTo("[nested] requires 'query' field"));

        e = expectThrows(IllegalArgumentException.class, () -> QueryBuilders.nestedQuery("foo", innerQuery, null));
        assertThat(e.getMessage(), equalTo("[nested] requires 'score_mode' field"));
    }

    public void testFromJson() throws IOException {
        String json = "{\n"
            + "  \"nested\" : {\n"
            + "    \"query\" : {\n"
            + "      \"bool\" : {\n"
            + "        \"must\" : [ {\n"
            + "          \"match\" : {\n"
            + "            \"obj1.name\" : {\n"
            + "              \"query\" : \"blue\",\n"
            + "              \"operator\" : \"OR\",\n"
            + "              \"prefix_length\" : 0,\n"
            + "              \"max_expansions\" : 50,\n"
            + "              \"fuzzy_transpositions\" : true,\n"
            + "              \"lenient\" : false,\n"
            + "              \"zero_terms_query\" : \"NONE\",\n"
            + "              \"auto_generate_synonyms_phrase_query\" : true,\n"
            + "              \"boost\" : 1.0\n"
            + "            }\n"
            + "          }\n"
            + "        }, {\n"
            + "          \"range\" : {\n"
            + "            \"obj1.count\" : {\n"
            + "              \"from\" : 5,\n"
            + "              \"to\" : null,\n"
            + "              \"include_lower\" : false,\n"
            + "              \"include_upper\" : true,\n"
            + "              \"boost\" : 1.0\n"
            + "            }\n"
            + "          }\n"
            + "        } ],\n"
            + "        \"adjust_pure_negative\" : true,\n"
            + "        \"boost\" : 1.0\n"
            + "      }\n"
            + "    },\n"
            + "    \"path\" : \"obj1\",\n"
            + "    \"ignore_unmapped\" : false,\n"
            + "    \"score_mode\" : \"avg\",\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";

        NestedQueryBuilder parsed = (NestedQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, ScoreMode.Avg, parsed.scoreMode());
    }

    @Override
    public void testMustRewrite() throws IOException {
        QueryShardContext context = createShardContext();
        context.setAllowUnmappedFields(true);
        TermQueryBuilder innerQueryBuilder = new TermQueryBuilder("nested1.unmapped_field", "foo");
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder(
            "nested1",
            innerQueryBuilder,
            RandomPicks.randomFrom(random(), ScoreMode.values())
        );
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> nestedQueryBuilder.toQuery(context));
        assertEquals("Rewrite first", e.getMessage());
    }

    public void testIgnoreUnmapped() throws IOException {
        final NestedQueryBuilder queryBuilder = new NestedQueryBuilder("unmapped", new MatchAllQueryBuilder(), ScoreMode.None);
        queryBuilder.ignoreUnmapped(true);
        Query query = queryBuilder.toQuery(createShardContext());
        assertThat(query, notNullValue());
        assertThat(query, instanceOf(MatchNoDocsQuery.class));

        final NestedQueryBuilder failingQueryBuilder = new NestedQueryBuilder("unmapped", new MatchAllQueryBuilder(), ScoreMode.None);
        failingQueryBuilder.ignoreUnmapped(false);
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> failingQueryBuilder.toQuery(createShardContext()));
        assertThat(e.getMessage(), containsString("[" + NestedQueryBuilder.NAME + "] failed to find nested object under path [unmapped]"));
    }

    public void testIgnoreUnmappedWithRewrite() throws IOException {
        // WrapperQueryBuilder makes sure we always rewrite
        final NestedQueryBuilder queryBuilder = new NestedQueryBuilder(
            "unmapped",
            new WrapperQueryBuilder(new MatchAllQueryBuilder().toString()),
            ScoreMode.None
        );
        queryBuilder.ignoreUnmapped(true);
        QueryShardContext queryShardContext = createShardContext();
        Query query = queryBuilder.rewrite(queryShardContext).toQuery(queryShardContext);
        assertThat(query, notNullValue());
        assertThat(query, instanceOf(MatchNoDocsQuery.class));
    }

    public void testMinFromString() {
        assertThat("fromString(min) != MIN", ScoreMode.Min, equalTo(NestedQueryBuilder.parseScoreMode("min")));
        assertThat("min", equalTo(NestedQueryBuilder.scoreModeAsString(ScoreMode.Min)));
    }

    public void testMaxFromString() {
        assertThat("fromString(max) != MAX", ScoreMode.Max, equalTo(NestedQueryBuilder.parseScoreMode("max")));
        assertThat("max", equalTo(NestedQueryBuilder.scoreModeAsString(ScoreMode.Max)));
    }

    public void testAvgFromString() {
        assertThat("fromString(avg) != AVG", ScoreMode.Avg, equalTo(NestedQueryBuilder.parseScoreMode("avg")));
        assertThat("avg", equalTo(NestedQueryBuilder.scoreModeAsString(ScoreMode.Avg)));
    }

    public void testSumFromString() {
        assertThat("fromString(total) != SUM", ScoreMode.Total, equalTo(NestedQueryBuilder.parseScoreMode("sum")));
        assertThat("sum", equalTo(NestedQueryBuilder.scoreModeAsString(ScoreMode.Total)));
    }

    public void testNoneFromString() {
        assertThat("fromString(none) != NONE", ScoreMode.None, equalTo(NestedQueryBuilder.parseScoreMode("none")));
        assertThat("none", equalTo(NestedQueryBuilder.scoreModeAsString(ScoreMode.None)));
    }

    /**
     * Should throw {@link IllegalArgumentException} instead of NPE.
     */
    public void testThatNullFromStringThrowsException() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> NestedQueryBuilder.parseScoreMode(null));
        assertEquals("No score mode for child query [null] found", e.getMessage());
    }

    /**
     * Failure should not change (and the value should never match anything...).
     */
    public void testThatUnrecognizedFromStringThrowsException() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> NestedQueryBuilder.parseScoreMode("unrecognized value")
        );
        assertEquals("No score mode for child query [unrecognized value] found", e.getMessage());
    }

    public void testInlineLeafInnerHitsNestedQuery() throws Exception {
        InnerHitBuilder leafInnerHits = randomNestedInnerHits();
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None);
        nestedQueryBuilder.innerHit(leafInnerHits);
        Map<String, InnerHitContextBuilder> innerHitBuilders = new HashMap<>();
        nestedQueryBuilder.extractInnerHitBuilders(innerHitBuilders);
        assertThat(innerHitBuilders.get(leafInnerHits.getName()), Matchers.notNullValue());
    }

    public void testParentFilterFromInlineLeafInnerHitsNestedQuery() throws Exception {
        QueryShardContext queryShardContext = createShardContext();
        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.getQueryShardContext()).thenReturn(queryShardContext);

        MapperService mapperService = mock(MapperService.class);
        IndexSettings settings = new IndexSettings(newIndexMeta("index", Settings.EMPTY), Settings.EMPTY);
        when(mapperService.getIndexSettings()).thenReturn(settings);
        when(searchContext.mapperService()).thenReturn(mapperService);

        InnerHitBuilder leafInnerHits = randomNestedInnerHits();
        // Set null for values not related with this test case
        leafInnerHits.setScriptFields(null);
        leafInnerHits.setHighlightBuilder(null);
        leafInnerHits.setSorts(null);

        QueryBuilder innerQueryBuilder = spy(new MatchAllQueryBuilder());
        when(innerQueryBuilder.toQuery(queryShardContext)).thenAnswer(invoke -> {
            QueryShardContext context = invoke.getArgument(0);
            if (context.getParentFilter() == null) {
                throw new Exception("Expect parent filter to be non-null");
            }
            return invoke.callRealMethod();
        });
        NestedQueryBuilder query = new NestedQueryBuilder("nested1", innerQueryBuilder, ScoreMode.None);
        query.innerHit(leafInnerHits);
        final Map<String, InnerHitContextBuilder> innerHitBuilders = new HashMap<>();
        final InnerHitsContext innerHitsContext = new InnerHitsContext();
        query.extractInnerHitBuilders(innerHitBuilders);
        assertThat(innerHitBuilders.size(), Matchers.equalTo(1));
        assertTrue(innerHitBuilders.containsKey(leafInnerHits.getName()));
        assertNull(queryShardContext.getParentFilter());
        innerHitBuilders.get(leafInnerHits.getName()).build(searchContext, innerHitsContext);
        assertNull(queryShardContext.getParentFilter());
        verify(innerQueryBuilder).toQuery(queryShardContext);
    }

    public void testInlineLeafInnerHitsNestedQueryViaBoolQuery() {
        InnerHitBuilder leafInnerHits = randomNestedInnerHits();
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None).innerHit(
            leafInnerHits
        );
        BoolQueryBuilder boolQueryBuilder = new BoolQueryBuilder().should(nestedQueryBuilder);
        Map<String, InnerHitContextBuilder> innerHitBuilders = new HashMap<>();
        boolQueryBuilder.extractInnerHitBuilders(innerHitBuilders);
        assertThat(innerHitBuilders.get(leafInnerHits.getName()), Matchers.notNullValue());
    }

    public void testInlineLeafInnerHitsNestedQueryViaConstantScoreQuery() {
        InnerHitBuilder leafInnerHits = randomNestedInnerHits();
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None).innerHit(
            leafInnerHits
        );
        ConstantScoreQueryBuilder constantScoreQueryBuilder = new ConstantScoreQueryBuilder(nestedQueryBuilder);
        Map<String, InnerHitContextBuilder> innerHitBuilders = new HashMap<>();
        constantScoreQueryBuilder.extractInnerHitBuilders(innerHitBuilders);
        assertThat(innerHitBuilders.get(leafInnerHits.getName()), Matchers.notNullValue());
    }

    public void testInlineLeafInnerHitsNestedQueryViaBoostingQuery() {
        InnerHitBuilder leafInnerHits1 = randomNestedInnerHits();
        NestedQueryBuilder nestedQueryBuilder1 = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None).innerHit(
            leafInnerHits1
        );
        InnerHitBuilder leafInnerHits2 = randomNestedInnerHits();
        NestedQueryBuilder nestedQueryBuilder2 = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None).innerHit(
            leafInnerHits2
        );
        BoostingQueryBuilder constantScoreQueryBuilder = new BoostingQueryBuilder(nestedQueryBuilder1, nestedQueryBuilder2);
        Map<String, InnerHitContextBuilder> innerHitBuilders = new HashMap<>();
        constantScoreQueryBuilder.extractInnerHitBuilders(innerHitBuilders);
        assertThat(innerHitBuilders.get(leafInnerHits1.getName()), Matchers.notNullValue());
        assertThat(innerHitBuilders.get(leafInnerHits2.getName()), Matchers.notNullValue());
    }

    public void testInlineLeafInnerHitsNestedQueryViaFunctionScoreQuery() {
        InnerHitBuilder leafInnerHits = randomNestedInnerHits();
        NestedQueryBuilder nestedQueryBuilder = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None).innerHit(
            leafInnerHits
        );
        FunctionScoreQueryBuilder functionScoreQueryBuilder = new FunctionScoreQueryBuilder(nestedQueryBuilder);
        Map<String, InnerHitContextBuilder> innerHitBuilders = new HashMap<>();
        ((AbstractQueryBuilder<?>) functionScoreQueryBuilder).extractInnerHitBuilders(innerHitBuilders);
        assertThat(innerHitBuilders.get(leafInnerHits.getName()), Matchers.notNullValue());
    }

    public void testBuildIgnoreUnmappedNestQuery() throws Exception {
        QueryShardContext queryShardContext = mock(QueryShardContext.class);
        when(queryShardContext.getObjectMapper("path")).thenReturn(null);
        SearchContext searchContext = mock(SearchContext.class);
        when(searchContext.getQueryShardContext()).thenReturn(queryShardContext);

        MapperService mapperService = mock(MapperService.class);
        IndexSettings settings = new IndexSettings(newIndexMeta("index", Settings.EMPTY), Settings.EMPTY);
        when(mapperService.getIndexSettings()).thenReturn(settings);
        when(searchContext.mapperService()).thenReturn(mapperService);

        InnerHitBuilder leafInnerHits = randomNestedInnerHits();
        NestedQueryBuilder query1 = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None);
        query1.innerHit(leafInnerHits);
        final Map<String, InnerHitContextBuilder> innerHitBuilders = new HashMap<>();
        final InnerHitsContext innerHitsContext = new InnerHitsContext();
        expectThrows(IllegalStateException.class, () -> {
            query1.extractInnerHitBuilders(innerHitBuilders);
            assertThat(innerHitBuilders.size(), Matchers.equalTo(1));
            assertTrue(innerHitBuilders.containsKey(leafInnerHits.getName()));
            innerHitBuilders.get(leafInnerHits.getName()).build(searchContext, innerHitsContext);
        });
        innerHitBuilders.clear();
        NestedQueryBuilder query2 = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None);
        query2.ignoreUnmapped(true);
        query2.innerHit(leafInnerHits);
        query2.extractInnerHitBuilders(innerHitBuilders);
        assertThat(innerHitBuilders.size(), Matchers.equalTo(1));
        assertTrue(innerHitBuilders.containsKey(leafInnerHits.getName()));
        assertThat(innerHitBuilders.get(leafInnerHits.getName()), instanceOf(NestedQueryBuilder.NestedInnerHitContextBuilder.class));
        NestedQueryBuilder.NestedInnerHitContextBuilder nestedContextBuilder =
            (NestedQueryBuilder.NestedInnerHitContextBuilder) innerHitBuilders.get(leafInnerHits.getName());
        nestedContextBuilder.build(searchContext, innerHitsContext);
        assertThat(innerHitsContext.getInnerHits().size(), Matchers.equalTo(0));
    }

    public void testExtractInnerHitBuildersWithDuplicate() {
        final NestedQueryBuilder queryBuilder = new NestedQueryBuilder(
            "path",
            new WrapperQueryBuilder(new MatchAllQueryBuilder().toString()),
            ScoreMode.None
        );
        queryBuilder.innerHit(new InnerHitBuilder("some_name"));
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> InnerHitContextBuilder.extractInnerHits(queryBuilder, Collections.singletonMap("some_name", null))
        );
        assertEquals("[inner_hits] already contains an entry for key [some_name]", e.getMessage());
    }

    public void testDisallowExpensiveQueries() {
        QueryShardContext queryShardContext = mock(QueryShardContext.class);
        when(queryShardContext.allowExpensiveQueries()).thenReturn(false);

        NestedQueryBuilder queryBuilder = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None);
        OpenSearchException e = expectThrows(OpenSearchException.class, () -> queryBuilder.toQuery(queryShardContext));
        assertEquals("[joining] queries cannot be executed when 'search.allow_expensive_queries' is set to false.", e.getMessage());
    }

    public void testSetParentFilterInContext() throws Exception {
        QueryShardContext queryShardContext = createShardContext();
        QueryBuilder innerQueryBuilder = spy(new MatchAllQueryBuilderTests().createTestQueryBuilder());
        when(innerQueryBuilder.toQuery(queryShardContext)).thenAnswer(invoke -> {
            QueryShardContext context = invoke.getArgument(0);
            if (context.getParentFilter() == null) {
                throw new Exception("Expect parent filter to be non-null");
            }
            return invoke.callRealMethod();
        });
        NestedQueryBuilder nqb = new NestedQueryBuilder("nested1", innerQueryBuilder, RandomPicks.randomFrom(random(), ScoreMode.values()));

        assertNull(queryShardContext.getParentFilter());
        nqb.rewrite(queryShardContext).toQuery(queryShardContext);
        assertNull(queryShardContext.getParentFilter());
        verify(innerQueryBuilder).toQuery(queryShardContext);
    }

    public void testNestedDepthProhibited() throws Exception {
        assertThrows(IllegalArgumentException.class, () -> doWithDepth(0, context -> fail("won't call")));
    }

    public void testNestedDepthAllowed() throws Exception {
        ThrowingConsumer<QueryShardContext> check = (context) -> {
            NestedQueryBuilder queryBuilder = new NestedQueryBuilder("nested1", new MatchAllQueryBuilder(), ScoreMode.None);
            OpenSearchToParentBlockJoinQuery blockJoinQuery = (OpenSearchToParentBlockJoinQuery) queryBuilder.toQuery(context);
            Optional<BooleanClause> childLeg = ((BooleanQuery) blockJoinQuery.getChildQuery()).clauses()
                .stream()
                .filter(c -> c.getOccur() == BooleanClause.Occur.MUST)
                .findFirst();
            assertTrue(childLeg.isPresent());
            assertEquals(new MatchAllDocsQuery(), childLeg.get().getQuery());
        };
        check.accept(createShardContext());
        doWithDepth(randomIntBetween(1, 20), check);
    }

    public void testNestedDepthOnceOnly() throws Exception {
        doWithDepth(1, this::checkOnceNested);
    }

    public void testNestedDepthDefault() throws Exception {
        assertEquals(Integer.MAX_VALUE, createShardContext().getIndexSettings().getMaxNestedQueryDepth());
    }

    private void checkOnceNested(QueryShardContext ctx) throws Exception {
        {
            NestedQueryBuilder depth2 = new NestedQueryBuilder(
                "nested1",
                new NestedQueryBuilder("nested1", new MatchAllQueryBuilder(), ScoreMode.None),
                ScoreMode.None
            );
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> depth2.toQuery(ctx));
            assertEquals(
                "The depth of Nested Query is [2] has exceeded the allowed maximum of [1]. This maximum can be set by changing the [index.query.max_nested_depth] index level setting.",
                e.getMessage()
            );
        }
        {
            QueryBuilder mustBjqMustBjq = new BoolQueryBuilder().must(
                new NestedQueryBuilder("nested1", new MatchAllQueryBuilder(), ScoreMode.None)
            ).must(new NestedQueryBuilder("nested1", new MatchAllQueryBuilder(), ScoreMode.None));
            BooleanQuery bool = (BooleanQuery) mustBjqMustBjq.toQuery(ctx);
            assertEquals(
                "Can parse joins one by one without breaching depth limit",
                2,
                bool.clauses().stream().filter(c -> c.getQuery() instanceof OpenSearchToParentBlockJoinQuery).count()
            );
        }
    }

    public void testUpdateMaxDepthSettings() throws Exception {
        doWithDepth(2, (ctx) -> {
            assertEquals(ctx.getIndexSettings().getMaxNestedQueryDepth(), 2);
            NestedQueryBuilder depth2 = new NestedQueryBuilder(
                "nested1",
                new NestedQueryBuilder("nested1", new MatchAllQueryBuilder(), ScoreMode.None),
                ScoreMode.None
            );
            Query depth2Query = depth2.toQuery(ctx);
            assertTrue(depth2Query instanceof OpenSearchToParentBlockJoinQuery);
        });
    }

    void doWithDepth(int depth, ThrowingConsumer<QueryShardContext> test) throws Exception {
        QueryShardContext context = createShardContext();
        int defLimit = context.getIndexSettings().getMaxNestedQueryDepth();
        assertTrue(defLimit > 0);
        Settings updateSettings = Settings.builder()
            .put(context.getIndexSettings().getSettings())
            .put("index.query.max_nested_depth", depth)
            .build();
        context.getIndexSettings().updateIndexMetadata(IndexMetadata.builder("index").settings(updateSettings).build());
        try {
            test.accept(context);
        } finally {
            context.getIndexSettings()
                .updateIndexMetadata(
                    IndexMetadata.builder("index")
                        .settings(
                            Settings.builder()
                                .put(context.getIndexSettings().getSettings())
                                .put("index.query.max_nested_depth", defLimit)
                                .build()
                        )
                        .build()
                );
        }
    }

    public void testVisit() {
        NestedQueryBuilder builder = new NestedQueryBuilder("path", new MatchAllQueryBuilder(), ScoreMode.None);

        List<QueryBuilder> visitedQueries = new ArrayList<>();
        builder.visit(createTestVisitor(visitedQueries));

        assertEquals(2, visitedQueries.size());
    }
}
