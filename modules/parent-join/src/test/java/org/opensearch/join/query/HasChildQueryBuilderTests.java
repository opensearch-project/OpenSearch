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

package org.opensearch.join.query;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermInSetQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.join.ScoreMode;
import org.apache.lucene.search.similarities.PerFieldSimilarityWrapper;
import org.apache.lucene.search.similarities.Similarity;
import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.IdsQueryBuilder;
import org.opensearch.index.query.InnerHitBuilder;
import org.opensearch.index.query.InnerHitContextBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.QueryShardException;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.WrapperQueryBuilder;
import org.opensearch.index.similarity.SimilarityService;
import org.opensearch.join.ParentJoinModulePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.AbstractQueryTestCase;
import org.opensearch.test.TestGeoShapeFieldMapperPlugin;
import org.opensearch.test.VersionUtils;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.join.query.JoinQueryBuilders.hasChildQuery;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.instanceOf;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HasChildQueryBuilderTests extends AbstractQueryTestCase<HasChildQueryBuilder> {

    private static final String TYPE = "_doc";
    private static final String PARENT_DOC = "parent";
    private static final String CHILD_DOC = "child";

    private static String similarity;

    boolean requiresRewrite = false;

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(ParentJoinModulePlugin.class, TestGeoShapeFieldMapperPlugin.class);
    }

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        similarity = randomFrom("boolean", "BM25");
        XContentBuilder mapping = jsonBuilder().startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("join_field")
            .field("type", "join")
            .startObject("relations")
            .field(PARENT_DOC, CHILD_DOC)
            .endObject()
            .endObject()
            .startObject(TEXT_FIELD_NAME)
            .field("type", "text")
            .endObject()
            .startObject(KEYWORD_FIELD_NAME)
            .field("type", "keyword")
            .endObject()
            .startObject(INT_FIELD_NAME)
            .field("type", "integer")
            .endObject()
            .startObject(DOUBLE_FIELD_NAME)
            .field("type", "double")
            .endObject()
            .startObject(BOOLEAN_FIELD_NAME)
            .field("type", "boolean")
            .endObject()
            .startObject(DATE_FIELD_NAME)
            .field("type", "date")
            .endObject()
            .startObject(OBJECT_FIELD_NAME)
            .field("type", "object")
            .endObject()
            .startObject("custom_string")
            .field("type", "text")
            .field("similarity", similarity)
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        mapperService.merge(TYPE, new CompressedXContent(mapping.toString()), MapperService.MergeReason.MAPPING_UPDATE);
    }

    /**
     * @return a {@link HasChildQueryBuilder} with random values all over the place
     */
    @Override
    protected HasChildQueryBuilder doCreateTestQueryBuilder() {
        int min = randomIntBetween(1, Integer.MAX_VALUE / 2);
        int max = randomIntBetween(min, Integer.MAX_VALUE);

        QueryBuilder innerQueryBuilder = new MatchAllQueryBuilder();
        if (randomBoolean()) {
            requiresRewrite = true;
            innerQueryBuilder = new WrapperQueryBuilder(innerQueryBuilder.toString());
        }

        HasChildQueryBuilder hqb = new HasChildQueryBuilder(
            CHILD_DOC,
            innerQueryBuilder,
            RandomPicks.randomFrom(random(), ScoreMode.values())
        );
        hqb.minMaxChildren(min, max);
        hqb.ignoreUnmapped(randomBoolean());
        if (randomBoolean()) {
            hqb.innerHit(
                new InnerHitBuilder().setName(randomAlphaOfLengthBetween(1, 10))
                    .setSize(randomIntBetween(0, 100))
                    .addSort(new FieldSortBuilder(KEYWORD_FIELD_NAME).order(SortOrder.ASC))
            );
        }
        return hqb;
    }

    public void testDeprecationOfZeroMinChildren() {
        QueryBuilder query = new MatchAllQueryBuilder();
        HasChildQueryBuilder foo = hasChildQuery("foo", query, ScoreMode.None);
        foo.minMaxChildren(0, 1);
        assertWarnings(HasChildQueryBuilder.MIN_CHILDREN_0_DEPRECATION_MESSAGE);
    }

    @Override
    protected void doAssertLuceneQuery(HasChildQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        assertThat(query, instanceOf(HasChildQueryBuilder.LateParsingQuery.class));
        HasChildQueryBuilder.LateParsingQuery lpq = (HasChildQueryBuilder.LateParsingQuery) query;
        assertEquals(queryBuilder.minChildren(), lpq.getMinChildren());
        assertEquals(queryBuilder.maxChildren(), lpq.getMaxChildren());
        assertEquals(queryBuilder.scoreMode(), lpq.getScoreMode()); // WTF is this why do we have two?
        if (queryBuilder.innerHit() != null) {
            // have to rewrite again because the provided queryBuilder hasn't been rewritten (directly returned from
            // doCreateTestQueryBuilder)
            queryBuilder = (HasChildQueryBuilder) queryBuilder.rewrite(context);
            Map<String, InnerHitContextBuilder> innerHitBuilders = new HashMap<>();
            InnerHitContextBuilder.extractInnerHits(queryBuilder, innerHitBuilders);
            assertTrue(innerHitBuilders.containsKey(queryBuilder.innerHit().getName()));
            InnerHitContextBuilder innerHits = innerHitBuilders.get(queryBuilder.innerHit().getName());
            assertEquals(innerHits.innerHitBuilder(), queryBuilder.innerHit());
        }
    }

    /**
     * Test (de)serialization on all previous released versions
     */
    public void testSerializationBWC() throws IOException {
        for (Version version : VersionUtils.allReleasedVersions()) {
            HasChildQueryBuilder testQuery = createTestQueryBuilder();
            assertSerialization(testQuery, version);
        }
    }

    public void testIllegalValues() {
        QueryBuilder query = new MatchAllQueryBuilder();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> hasChildQuery(null, query, ScoreMode.None));
        assertEquals("[has_child] requires 'type' field", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> hasChildQuery("foo", null, ScoreMode.None));
        assertEquals("[has_child] requires 'query' field", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> hasChildQuery("foo", query, null));
        assertEquals("[has_child] requires 'score_mode' field", e.getMessage());

        int positiveValue = randomIntBetween(0, Integer.MAX_VALUE);
        HasChildQueryBuilder foo = hasChildQuery("foo", query, ScoreMode.None); // all good
        e = expectThrows(IllegalArgumentException.class, () -> foo.minMaxChildren(randomIntBetween(Integer.MIN_VALUE, -1), positiveValue));
        assertEquals("[has_child] requires non-negative 'min_children' field", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> foo.minMaxChildren(positiveValue, randomIntBetween(Integer.MIN_VALUE, -1)));
        assertEquals("[has_child] requires non-negative 'max_children' field", e.getMessage());

        e = expectThrows(IllegalArgumentException.class, () -> foo.minMaxChildren(positiveValue, positiveValue - 10));
        assertEquals("[has_child] 'max_children' is less than 'min_children'", e.getMessage());
    }

    public void testFromJson() throws IOException {
        String query = "{\n"
            + "  \"has_child\" : {\n"
            + "    \"query\" : {\n"
            + "      \"range\" : {\n"
            + "        \"mapped_string\" : {\n"
            + "          \"from\" : \"agJhRET\",\n"
            + "          \"to\" : \"zvqIq\",\n"
            + "          \"include_lower\" : true,\n"
            + "          \"include_upper\" : true,\n"
            + "          \"boost\" : 1.0\n"
            + "        }\n"
            + "      }\n"
            + "    },\n"
            + "    \"type\" : \"child\",\n"
            + "    \"score_mode\" : \"avg\",\n"
            + "    \"min_children\" : 883170873,\n"
            + "    \"max_children\" : 1217235442,\n"
            + "    \"ignore_unmapped\" : false,\n"
            + "    \"boost\" : 2.0,\n"
            + "    \"_name\" : \"WNzYMJKRwePuRBh\",\n"
            + "    \"inner_hits\" : {\n"
            + "      \"name\" : \"inner_hits_name\",\n"
            + "      \"ignore_unmapped\" : false,\n"
            + "      \"from\" : 0,\n"
            + "      \"size\" : 100,\n"
            + "      \"version\" : false,\n"
            + "      \"seq_no_primary_term\" : false,\n"
            + "      \"explain\" : false,\n"
            + "      \"track_scores\" : false,\n"
            + "      \"sort\" : [ {\n"
            + "        \"mapped_string\" : {\n"
            + "          \"order\" : \"asc\"\n"
            + "        }\n"
            + "      } ]\n"
            + "    }\n"
            + "  }\n"
            + "}";
        HasChildQueryBuilder queryBuilder = (HasChildQueryBuilder) parseQuery(query);
        checkGeneratedJson(query, queryBuilder);
        assertEquals(query, queryBuilder.maxChildren(), 1217235442);
        assertEquals(query, queryBuilder.minChildren(), 883170873);
        assertEquals(query, queryBuilder.boost(), 2.0f, 0.0f);
        assertEquals(query, queryBuilder.queryName(), "WNzYMJKRwePuRBh");
        assertEquals(query, queryBuilder.childType(), "child");
        assertEquals(query, queryBuilder.scoreMode(), ScoreMode.Avg);
        assertNotNull(query, queryBuilder.innerHit());
        InnerHitBuilder expected = new InnerHitBuilder("child").setName("inner_hits_name")
            .setSize(100)
            .addSort(new FieldSortBuilder("mapped_string").order(SortOrder.ASC));
        assertEquals(query, queryBuilder.innerHit(), expected);
    }

    public void testToQueryInnerQueryType() throws IOException {
        QueryShardContext shardContext = createShardContext();
        HasChildQueryBuilder hasChildQueryBuilder = hasChildQuery(CHILD_DOC, new IdsQueryBuilder().addIds("id"), ScoreMode.None);
        Query query = hasChildQueryBuilder.toQuery(shardContext);
        assertLateParsingQuery(query, CHILD_DOC, "id");
    }

    static void assertLateParsingQuery(Query query, String type, String id) throws IOException {
        assertThat(query, instanceOf(HasChildQueryBuilder.LateParsingQuery.class));
        HasChildQueryBuilder.LateParsingQuery lateParsingQuery = (HasChildQueryBuilder.LateParsingQuery) query;
        assertThat(lateParsingQuery.getInnerQuery(), instanceOf(BooleanQuery.class));
        BooleanQuery booleanQuery = (BooleanQuery) lateParsingQuery.getInnerQuery();
        assertThat(booleanQuery.clauses().size(), equalTo(2));
        // check the inner ids query, we have to call rewrite to get to check the type it's executed against
        assertThat(booleanQuery.clauses().get(0).occur(), equalTo(BooleanClause.Occur.MUST));
        assertThat(booleanQuery.clauses().get(0).query(), instanceOf(TermInSetQuery.class));
        TermInSetQuery termsQuery = (TermInSetQuery) booleanQuery.clauses().get(0).query();
        // The query is of type MultiTermQueryConstantScoreBlendedWrapper and is sealed inside Apache Lucene,
        // no access to inner queries without using the reflection, falling back to stringified query comparison
        assertThat(termsQuery.toString(), equalTo("_id:([ff 69 64])"));
        // check the type filter
        assertThat(booleanQuery.clauses().get(1).occur(), equalTo(BooleanClause.Occur.FILTER));
        assertEquals(new TermQuery(new Term("join_field", type)), booleanQuery.clauses().get(1).query());
    }

    @Override
    public void testMustRewrite() throws IOException {
        try {
            super.testMustRewrite();
        } catch (UnsupportedOperationException e) {
            if (requiresRewrite == false) {
                throw e;
            }
        }
    }

    public void testNonDefaultSimilarity() throws Exception {
        QueryShardContext shardContext = createShardContext();
        HasChildQueryBuilder hasChildQueryBuilder = hasChildQuery(
            CHILD_DOC,
            new TermQueryBuilder("custom_string", "value"),
            ScoreMode.None
        );
        HasChildQueryBuilder.LateParsingQuery query = (HasChildQueryBuilder.LateParsingQuery) hasChildQueryBuilder.toQuery(shardContext);
        Similarity expected = SimilarityService.BUILT_IN.get(similarity).apply(Settings.EMPTY, Version.CURRENT, null);
        assertThat(((PerFieldSimilarityWrapper) query.getSimilarity()).get("custom_string"), instanceOf(expected.getClass()));
    }

    public void testIgnoreUnmapped() throws IOException {
        final HasChildQueryBuilder queryBuilder = new HasChildQueryBuilder("unmapped", new MatchAllQueryBuilder(), ScoreMode.None);
        queryBuilder.innerHit(new InnerHitBuilder());
        assertFalse(queryBuilder.innerHit().isIgnoreUnmapped());
        queryBuilder.ignoreUnmapped(true);
        assertTrue(queryBuilder.innerHit().isIgnoreUnmapped());
        Query query = queryBuilder.toQuery(createShardContext());
        assertThat(query, notNullValue());
        assertThat(query, instanceOf(MatchNoDocsQuery.class));

        final HasChildQueryBuilder failingQueryBuilder = new HasChildQueryBuilder("unmapped", new MatchAllQueryBuilder(), ScoreMode.None);
        failingQueryBuilder.innerHit(new InnerHitBuilder());
        assertFalse(failingQueryBuilder.innerHit().isIgnoreUnmapped());
        failingQueryBuilder.ignoreUnmapped(false);
        assertFalse(failingQueryBuilder.innerHit().isIgnoreUnmapped());
        QueryShardException e = expectThrows(QueryShardException.class, () -> failingQueryBuilder.toQuery(createShardContext()));
        assertThat(
            e.getMessage(),
            containsString("[" + HasChildQueryBuilder.NAME + "] join field [join_field] doesn't hold [unmapped] as a child")
        );
    }

    public void testIgnoreUnmappedWithRewrite() throws IOException {
        // WrapperQueryBuilder makes sure we always rewrite
        final HasChildQueryBuilder queryBuilder = new HasChildQueryBuilder(
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

    public void testExtractInnerHitBuildersWithDuplicate() {
        final HasChildQueryBuilder queryBuilder = new HasChildQueryBuilder(
            CHILD_DOC,
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

        HasChildQueryBuilder queryBuilder = hasChildQuery(CHILD_DOC, new TermQueryBuilder("custom_string", "value"), ScoreMode.None);
        OpenSearchException e = expectThrows(OpenSearchException.class, () -> queryBuilder.toQuery(queryShardContext));
        assertEquals("[joining] queries cannot be executed when 'search.allow_expensive_queries' is set to false.", e.getMessage());
    }
}
