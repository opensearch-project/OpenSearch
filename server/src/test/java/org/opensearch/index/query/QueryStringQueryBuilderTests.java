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

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.document.SortedNumericDocValuesField;
import org.apache.lucene.index.Term;
import org.apache.lucene.queries.BlendedTermQuery;
import org.apache.lucene.queries.spans.SpanNearQuery;
import org.apache.lucene.queries.spans.SpanOrQuery;
import org.apache.lucene.queries.spans.SpanTermQuery;
import org.apache.lucene.search.AutomatonQuery;
import org.apache.lucene.search.BooleanClause;
import org.apache.lucene.search.BooleanClause.Occur;
import org.apache.lucene.search.BooleanQuery;
import org.apache.lucene.search.BoostQuery;
import org.apache.lucene.search.ConstantScoreQuery;
import org.apache.lucene.search.DisjunctionMaxQuery;
import org.apache.lucene.search.FuzzyQuery;
import org.apache.lucene.search.IndexOrDocValuesQuery;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.MultiTermQuery;
import org.apache.lucene.search.NormsFieldExistsQuery;
import org.apache.lucene.search.PhraseQuery;
import org.apache.lucene.search.PrefixQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.RegexpQuery;
import org.apache.lucene.search.SynonymQuery;
import org.apache.lucene.search.TermQuery;
import org.apache.lucene.search.TermRangeQuery;
import org.apache.lucene.search.WildcardQuery;
import org.apache.lucene.tests.analysis.MockSynonymAnalyzer;
import org.apache.lucene.util.BytesRef;
import org.apache.lucene.util.automaton.Automata;
import org.apache.lucene.util.automaton.Automaton;
import org.apache.lucene.util.automaton.Operations;
import org.apache.lucene.util.automaton.TooComplexToDeterminizeException;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.FieldNamesFieldMapper;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.search.QueryStringQueryParser;
import org.opensearch.search.approximate.ApproximatePointRangeQuery;
import org.opensearch.search.approximate.ApproximateScoreQuery;
import org.opensearch.test.AbstractQueryTestCase;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matchers;

import java.io.IOException;
import java.time.DateTimeException;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.query.AbstractQueryBuilder.parseInnerQueryBuilder;
import static org.opensearch.index.query.QueryBuilders.queryStringQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertBooleanSubQuery;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;
import static org.apache.lucene.document.LongPoint.pack;
import static org.junit.Assume.assumeThat;

public class QueryStringQueryBuilderTests extends AbstractQueryTestCase<QueryStringQueryBuilder> {

    @Override
    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {
        XContentBuilder mapping = jsonBuilder().startObject()
            .startObject("_doc")
            .startObject("properties")
            .startObject("prefix_field")
            .field("type", "text")
            .startObject("index_prefixes")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject();

        mapperService.merge("_doc", new CompressedXContent(mapping.toString()), MapperService.MergeReason.MAPPING_UPDATE);
    }

    @Override
    protected QueryStringQueryBuilder doCreateTestQueryBuilder() {
        int numTerms = randomIntBetween(0, 5);
        String query = "";
        for (int i = 0; i < numTerms; i++) {
            // min length 4 makes sure that the text is not an operator (AND/OR) so toQuery won't break
            // also avoid "now" since we might hit dqte fields later and this complicates caching checks
            String term = randomValueOtherThanMany(
                s -> s.toLowerCase(Locale.ROOT).contains("now"),
                () -> randomAlphaOfLengthBetween(4, 10)
            );
            query += (randomBoolean() ? TEXT_FIELD_NAME + ":" : "") + term + " ";
        }
        QueryStringQueryBuilder queryStringQueryBuilder = new QueryStringQueryBuilder(query);
        if (randomBoolean()) {
            String defaultFieldName = randomFrom(TEXT_FIELD_NAME, TEXT_ALIAS_FIELD_NAME, randomAlphaOfLengthBetween(1, 10));
            queryStringQueryBuilder.defaultField(defaultFieldName);
        } else {
            int numFields = randomIntBetween(1, 5);
            for (int i = 0; i < numFields; i++) {
                String fieldName = randomFrom(TEXT_FIELD_NAME, TEXT_ALIAS_FIELD_NAME, randomAlphaOfLengthBetween(1, 10));
                if (randomBoolean()) {
                    queryStringQueryBuilder.field(fieldName);
                } else {
                    queryStringQueryBuilder.field(fieldName, randomFloat());
                }
            }
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.defaultOperator(randomFrom(Operator.values()));
        }
        if (randomBoolean()) {
            // we only use string fields (either mapped or unmapped)
            queryStringQueryBuilder.fuzziness(randomFuzziness(TEXT_FIELD_NAME));
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.analyzer(randomAnalyzer());
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.quoteAnalyzer(randomAnalyzer());
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.allowLeadingWildcard(randomBoolean());
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.analyzeWildcard(randomBoolean());
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.maxDeterminizedStates(randomIntBetween(1, 100));
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.enablePositionIncrements(randomBoolean());
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.escape(randomBoolean());
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.phraseSlop(randomIntBetween(0, 10));
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.fuzzyMaxExpansions(randomIntBetween(0, 100));
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.fuzzyPrefixLength(randomIntBetween(0, 10));
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.fuzzyRewrite(getRandomRewriteMethod());
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.rewrite(getRandomRewriteMethod());
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.quoteFieldSuffix(randomAlphaOfLengthBetween(1, 3));
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.tieBreaker((float) randomDoubleBetween(0d, 1d, true));
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.minimumShouldMatch(randomMinimumShouldMatch());
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.timeZone(randomZone().getId());
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.autoGenerateSynonymsPhraseQuery(randomBoolean());
        }
        if (randomBoolean()) {
            queryStringQueryBuilder.fuzzyTranspositions(randomBoolean());
        }
        queryStringQueryBuilder.type(randomFrom(MultiMatchQueryBuilder.Type.values()));
        return queryStringQueryBuilder;
    }

    @Override
    public QueryStringQueryBuilder mutateInstance(QueryStringQueryBuilder instance) throws IOException {
        String query = instance.queryString();
        String defaultField = instance.defaultField();
        Map<String, Float> fields = instance.fields();
        Operator operator = instance.defaultOperator();
        Fuzziness fuzziness = instance.fuzziness();
        String analyzer = instance.analyzer();
        String quoteAnalyzer = instance.quoteAnalyzer();
        Boolean allowLeadingWildCard = instance.allowLeadingWildcard();
        Boolean analyzeWildcard = instance.analyzeWildcard();
        int maxDeterminizedStates = instance.maxDeterminizedStates();
        boolean enablePositionIncrements = instance.enablePositionIncrements();
        boolean escape = instance.escape();
        int phraseSlop = instance.phraseSlop();
        int fuzzyMaxExpansions = instance.fuzzyMaxExpansions();
        int fuzzyPrefixLength = instance.fuzzyPrefixLength();
        String fuzzyRewrite = instance.fuzzyRewrite();
        String rewrite = instance.rewrite();
        String quoteFieldSuffix = instance.quoteFieldSuffix();
        Float tieBreaker = instance.tieBreaker();
        String minimumShouldMatch = instance.minimumShouldMatch();
        String timeZone = instance.timeZone() == null ? null : instance.timeZone().getId();
        boolean autoGenerateSynonymsPhraseQuery = instance.autoGenerateSynonymsPhraseQuery();
        boolean fuzzyTranspositions = instance.fuzzyTranspositions();

        switch (between(0, 23)) {
            case 0:
                query = query + " foo";
                break;
            case 1:
                if (defaultField == null) {
                    defaultField = randomAlphaOfLengthBetween(1, 10);
                } else {
                    defaultField = defaultField + randomAlphaOfLength(5);
                }
                break;
            case 2:
                fields = new HashMap<>(fields);
                fields.put(randomAlphaOfLength(10), 1.0f);
                break;
            case 3:
                operator = randomValueOtherThan(operator, () -> randomFrom(Operator.values()));
                break;
            case 4:
                fuzziness = randomValueOtherThan(fuzziness, () -> randomFrom(Fuzziness.AUTO, Fuzziness.ZERO, Fuzziness.ONE, Fuzziness.TWO));
                break;
            case 5:
                if (analyzer == null) {
                    analyzer = randomAnalyzer();
                } else {
                    analyzer = null;
                }
                break;
            case 6:
                if (quoteAnalyzer == null) {
                    quoteAnalyzer = randomAnalyzer();
                } else {
                    quoteAnalyzer = null;
                }
                break;
            case 7:
                if (allowLeadingWildCard == null) {
                    allowLeadingWildCard = randomBoolean();
                } else {
                    allowLeadingWildCard = randomBoolean() ? null : (allowLeadingWildCard == false);
                }
                break;
            case 8:
                if (analyzeWildcard == null) {
                    analyzeWildcard = randomBoolean();
                } else {
                    analyzeWildcard = randomBoolean() ? null : (analyzeWildcard == false);
                }
                break;
            case 9:
                maxDeterminizedStates += 5;
                break;
            case 10:
                enablePositionIncrements = (enablePositionIncrements == false);
                break;
            case 11:
                escape = (escape == false);
                break;
            case 12:
                phraseSlop += 5;
                break;
            case 13:
                fuzzyMaxExpansions += 5;
                break;
            case 14:
                fuzzyPrefixLength += 5;
                break;
            case 15:
                if (fuzzyRewrite == null) {
                    fuzzyRewrite = getRandomRewriteMethod();
                } else {
                    fuzzyRewrite = null;
                }
                break;
            case 16:
                if (rewrite == null) {
                    rewrite = getRandomRewriteMethod();
                } else {
                    rewrite = null;
                }
                break;
            case 17:
                if (quoteFieldSuffix == null) {
                    quoteFieldSuffix = randomAlphaOfLengthBetween(1, 3);
                } else {
                    quoteFieldSuffix = quoteFieldSuffix + randomAlphaOfLength(1);
                }
                break;
            case 18:
                if (tieBreaker == null) {
                    tieBreaker = randomFloat();
                } else {
                    tieBreaker += 0.05f;
                }
                break;
            case 19:
                if (minimumShouldMatch == null) {
                    minimumShouldMatch = randomMinimumShouldMatch();
                } else {
                    minimumShouldMatch = null;
                }
                break;
            case 20:
                if (timeZone == null) {
                    timeZone = randomZone().getId();
                } else {
                    if (randomBoolean()) {
                        timeZone = null;
                    } else {
                        timeZone = randomValueOtherThan(timeZone, () -> randomZone().getId());
                    }
                }
                break;
            case 21:
                autoGenerateSynonymsPhraseQuery = (autoGenerateSynonymsPhraseQuery == false);
                break;
            case 22:
                fuzzyTranspositions = (fuzzyTranspositions == false);
                break;
            case 23:
                return changeNameOrBoost(instance);
            default:
                throw new AssertionError("Illegal randomisation branch");
        }

        QueryStringQueryBuilder newInstance = new QueryStringQueryBuilder(query);
        if (defaultField != null) {
            newInstance.defaultField(defaultField);
        }
        newInstance.fields(fields);
        newInstance.defaultOperator(operator);
        newInstance.fuzziness(fuzziness);
        if (analyzer != null) {
            newInstance.analyzer(analyzer);
        }
        if (quoteAnalyzer != null) {
            newInstance.quoteAnalyzer(quoteAnalyzer);
        }
        if (allowLeadingWildCard != null) {
            newInstance.allowLeadingWildcard(allowLeadingWildCard);
        }
        if (analyzeWildcard != null) {
            newInstance.analyzeWildcard(analyzeWildcard);
        }
        newInstance.maxDeterminizedStates(maxDeterminizedStates);
        newInstance.enablePositionIncrements(enablePositionIncrements);
        newInstance.escape(escape);
        newInstance.phraseSlop(phraseSlop);
        newInstance.fuzzyMaxExpansions(fuzzyMaxExpansions);
        newInstance.fuzzyPrefixLength(fuzzyPrefixLength);
        if (fuzzyRewrite != null) {
            newInstance.fuzzyRewrite(fuzzyRewrite);
        }
        if (rewrite != null) {
            newInstance.rewrite(rewrite);
        }
        if (quoteFieldSuffix != null) {
            newInstance.quoteFieldSuffix(quoteFieldSuffix);
        }
        if (tieBreaker != null) {
            newInstance.tieBreaker(tieBreaker);
        }
        if (minimumShouldMatch != null) {
            newInstance.minimumShouldMatch(minimumShouldMatch);
        }
        if (timeZone != null) {
            newInstance.timeZone(timeZone);
        }
        newInstance.autoGenerateSynonymsPhraseQuery(autoGenerateSynonymsPhraseQuery);
        newInstance.fuzzyTranspositions(fuzzyTranspositions);

        return newInstance;
    }

    @Override
    protected void doAssertLuceneQuery(QueryStringQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        // nothing yet, put additional assertions here.
    }

    // Tests fix for https://github.com/elastic/elasticsearch/issues/29403
    public void testTimezoneEquals() {
        QueryStringQueryBuilder builder1 = new QueryStringQueryBuilder("bar");
        QueryStringQueryBuilder builder2 = new QueryStringQueryBuilder("foo");
        assertNotEquals(builder1, builder2);
        builder1.timeZone("Europe/London");
        builder2.timeZone("Europe/London");
        assertNotEquals(builder1, builder2);
    }

    public void testIllegalArguments() {
        expectThrows(IllegalArgumentException.class, () -> new QueryStringQueryBuilder((String) null));
    }

    public void testToQueryMatchAllQuery() throws Exception {
        Query query = queryStringQuery("*:*").toQuery(createShardContext());
        assertThat(query, instanceOf(MatchAllDocsQuery.class));
    }

    public void testToQueryTermQuery() throws IOException {
        Query query = queryStringQuery("test").defaultField(TEXT_FIELD_NAME).toQuery(createShardContext());
        assertThat(query, instanceOf(TermQuery.class));
        TermQuery termQuery = (TermQuery) query;
        assertThat(termQuery.getTerm(), equalTo(new Term(TEXT_FIELD_NAME, "test")));
    }

    public void testToQueryPhraseQuery() throws IOException {
        Query query = queryStringQuery("\"term1 term2\"").defaultField(TEXT_FIELD_NAME).phraseSlop(3).toQuery(createShardContext());
        assertThat(query, instanceOf(PhraseQuery.class));
        PhraseQuery phraseQuery = (PhraseQuery) query;
        assertThat(phraseQuery.getTerms().length, equalTo(2));
        assertThat(phraseQuery.getTerms()[0], equalTo(new Term(TEXT_FIELD_NAME, "term1")));
        assertThat(phraseQuery.getTerms()[1], equalTo(new Term(TEXT_FIELD_NAME, "term2")));
        assertThat(phraseQuery.getSlop(), equalTo(3));
    }

    public void testToQueryBoosts() throws Exception {
        QueryShardContext shardContext = createShardContext();
        QueryStringQueryBuilder queryStringQuery = queryStringQuery(TEXT_FIELD_NAME + ":boosted^2");
        Query query = queryStringQuery.toQuery(shardContext);
        assertThat(query, instanceOf(BoostQuery.class));
        BoostQuery boostQuery = (BoostQuery) query;
        assertThat(boostQuery.getBoost(), equalTo(2.0f));
        assertThat(boostQuery.getQuery(), instanceOf(TermQuery.class));
        assertThat(((TermQuery) boostQuery.getQuery()).getTerm(), equalTo(new Term(TEXT_FIELD_NAME, "boosted")));
        queryStringQuery.boost(2.0f);
        query = queryStringQuery.toQuery(shardContext);
        assertThat(query, instanceOf(BoostQuery.class));
        boostQuery = (BoostQuery) query;
        assertThat(boostQuery.getBoost(), equalTo(2.0f));
        assertThat(boostQuery.getQuery(), instanceOf(BoostQuery.class));
        boostQuery = (BoostQuery) boostQuery.getQuery();
        assertThat(boostQuery.getBoost(), equalTo(2.0f));

        queryStringQuery = queryStringQuery("((" + TEXT_FIELD_NAME + ":boosted^2) AND (" + TEXT_FIELD_NAME + ":foo^1.5))^3");
        query = queryStringQuery.toQuery(shardContext);
        assertThat(query, instanceOf(BoostQuery.class));
        boostQuery = (BoostQuery) query;
        assertThat(boostQuery.getBoost(), equalTo(3.0f));
        BoostQuery boostQuery1 = assertBooleanSubQuery(boostQuery.getQuery(), BoostQuery.class, 0);
        assertThat(boostQuery1.getBoost(), equalTo(2.0f));
        assertThat(boostQuery1.getQuery(), instanceOf(TermQuery.class));
        assertThat(((TermQuery) boostQuery1.getQuery()).getTerm(), equalTo(new Term(TEXT_FIELD_NAME, "boosted")));
        BoostQuery boostQuery2 = assertBooleanSubQuery(boostQuery.getQuery(), BoostQuery.class, 1);
        assertThat(boostQuery2.getBoost(), equalTo(1.5f));
        assertThat(boostQuery2.getQuery(), instanceOf(TermQuery.class));
        assertThat(((TermQuery) boostQuery2.getQuery()).getTerm(), equalTo(new Term(TEXT_FIELD_NAME, "foo")));
        queryStringQuery.boost(2.0f);
        query = queryStringQuery.toQuery(shardContext);
        assertThat(query, instanceOf(BoostQuery.class));
        boostQuery = (BoostQuery) query;
        assertThat(boostQuery.getBoost(), equalTo(2.0f));
    }

    public void testToQueryMultipleTermsBooleanQuery() throws Exception {
        Query query = queryStringQuery("test1 test2").field(TEXT_FIELD_NAME).toQuery(createShardContext());
        assertThat(query, instanceOf(BooleanQuery.class));
        BooleanQuery bQuery = (BooleanQuery) query;
        assertThat(bQuery.clauses().size(), equalTo(2));
        assertThat(assertBooleanSubQuery(query, TermQuery.class, 0).getTerm(), equalTo(new Term(TEXT_FIELD_NAME, "test1")));
        assertThat(assertBooleanSubQuery(query, TermQuery.class, 1).getTerm(), equalTo(new Term(TEXT_FIELD_NAME, "test2")));
    }

    public void testToQueryMultipleFieldsBooleanQuery() throws Exception {
        Query query = queryStringQuery("test").field(TEXT_FIELD_NAME).field(KEYWORD_FIELD_NAME).toQuery(createShardContext());
        Query expected = new DisjunctionMaxQuery(
            List.of(new TermQuery(new Term(TEXT_FIELD_NAME, "test")), new TermQuery(new Term(KEYWORD_FIELD_NAME, "test"))),
            0
        );
        assertEquals(expected, query);
    }

    public void testToQueryMultipleFieldsDisMaxQuery() throws Exception {
        Query query = queryStringQuery("test").field(TEXT_FIELD_NAME).field(KEYWORD_FIELD_NAME).toQuery(createShardContext());
        Query expected = new DisjunctionMaxQuery(
            List.of(new TermQuery(new Term(TEXT_FIELD_NAME, "test")), new TermQuery(new Term(KEYWORD_FIELD_NAME, "test"))),
            0
        );
        assertEquals(expected, query);
    }

    public void testToQueryFieldsWildcard() throws Exception {
        Query query = queryStringQuery("test").field("mapped_str*").toQuery(createShardContext());
        Query expected = new DisjunctionMaxQuery(
            List.of(new TermQuery(new Term(TEXT_FIELD_NAME, "test")), new TermQuery(new Term(KEYWORD_FIELD_NAME, "test"))),
            0
        );
        assertEquals(expected, query);
    }

    /**
     * Test that dissalowing leading wildcards causes exception
     */
    public void testAllowLeadingWildcard() throws Exception {
        Query query = queryStringQuery("*test").field("mapped_string").allowLeadingWildcard(true).toQuery(createShardContext());
        assertThat(query, instanceOf(WildcardQuery.class));
        QueryShardException ex = expectThrows(
            QueryShardException.class,
            () -> queryStringQuery("*test").field("mapped_string").allowLeadingWildcard(false).toQuery(createShardContext())
        );
        assertEquals("Failed to parse query [*test]", ex.getMessage());
        assertEquals("Cannot parse '*test': '*' or '?' not allowed as first character in WildcardQuery", ex.getCause().getMessage());
    }

    public void testToQueryDisMaxQuery() throws Exception {
        Query query = queryStringQuery("test").field(TEXT_FIELD_NAME, 2.2f).field(KEYWORD_FIELD_NAME).toQuery(createShardContext());
        Query expected = new DisjunctionMaxQuery(
            List.of(
                new BoostQuery(new TermQuery(new Term(TEXT_FIELD_NAME, "test")), 2.2f),
                new TermQuery(new Term(KEYWORD_FIELD_NAME, "test"))
            ),
            0
        );
        assertEquals(expected, query);
    }

    public void testToQueryWildcardQuery() throws Exception {
        for (Operator op : Operator.values()) {
            BooleanClause.Occur defaultOp = op.toBooleanClauseOccur();
            QueryStringQueryParser queryParser = new QueryStringQueryParser(createShardContext(), TEXT_FIELD_NAME);
            queryParser.setAnalyzeWildcard(true);
            queryParser.setMultiTermRewriteMethod(MultiTermQuery.CONSTANT_SCORE_REWRITE);
            queryParser.setDefaultOperator(op.toQueryParserOperator());
            Query query = queryParser.parse("first foo-bar-foobar* last");
            Query expectedQuery = new BooleanQuery.Builder().add(
                new BooleanClause(new TermQuery(new Term(TEXT_FIELD_NAME, "first")), defaultOp)
            )
                .add(
                    new BooleanQuery.Builder().add(new BooleanClause(new TermQuery(new Term(TEXT_FIELD_NAME, "foo")), defaultOp))
                        .add(new BooleanClause(new TermQuery(new Term(TEXT_FIELD_NAME, "bar")), defaultOp))
                        .add(
                            new BooleanClause(
                                new PrefixQuery(new Term(TEXT_FIELD_NAME, "foobar"), MultiTermQuery.CONSTANT_SCORE_REWRITE),
                                defaultOp
                            )
                        )
                        .build(),
                    defaultOp
                )
                .add(new BooleanClause(new TermQuery(new Term(TEXT_FIELD_NAME, "last")), defaultOp))
                .build();
            assertThat(query, Matchers.equalTo(expectedQuery));
        }
    }

    public void testToQueryWildcardWithIndexedPrefixes() throws Exception {
        QueryStringQueryParser queryParser = new QueryStringQueryParser(createShardContext(), "prefix_field");
        queryParser.setMultiTermRewriteMethod(MultiTermQuery.CONSTANT_SCORE_REWRITE);

        Query query = queryParser.parse("foo*");
        Query expectedQuery = new ConstantScoreQuery(new TermQuery(new Term("prefix_field._index_prefix", "foo")));
        assertThat(query, equalTo(expectedQuery));

        query = queryParser.parse("g*");
        Automaton a = Operations.concatenate(Arrays.asList(Automata.makeChar('g'), Automata.makeAnyChar()));
        expectedQuery = new ConstantScoreQuery(
            new BooleanQuery.Builder().add(
                new AutomatonQuery(
                    new Term("prefix_field._index_prefix", "g*"),
                    a,
                    Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
                    false,
                    MultiTermQuery.CONSTANT_SCORE_REWRITE
                ),
                Occur.SHOULD
            ).add(new TermQuery(new Term("prefix_field", "g")), Occur.SHOULD).build()
        );
        assertThat(query, equalTo(expectedQuery));
    }

    public void testToQueryWildcardQueryWithSynonyms() throws Exception {
        for (Operator op : Operator.values()) {
            BooleanClause.Occur defaultOp = op.toBooleanClauseOccur();
            QueryStringQueryParser queryParser = new QueryStringQueryParser(createShardContext(), TEXT_FIELD_NAME);
            queryParser.setAnalyzeWildcard(true);
            queryParser.setMultiTermRewriteMethod(MultiTermQuery.CONSTANT_SCORE_BLENDED_REWRITE);
            queryParser.setDefaultOperator(op.toQueryParserOperator());
            queryParser.setForceAnalyzer(new MockRepeatAnalyzer());
            Query query = queryParser.parse("first foo-bar-foobar* last");

            Query expectedQuery = new BooleanQuery.Builder().add(
                new BooleanClause(
                    new SynonymQuery.Builder(TEXT_FIELD_NAME).addTerm(new Term(TEXT_FIELD_NAME, "first"))
                        .addTerm(new Term(TEXT_FIELD_NAME, "first"))
                        .build(),
                    defaultOp
                )
            )
                .add(
                    new BooleanQuery.Builder().add(
                        new BooleanClause(
                            new SynonymQuery.Builder(TEXT_FIELD_NAME).addTerm(new Term(TEXT_FIELD_NAME, "foo"))
                                .addTerm(new Term(TEXT_FIELD_NAME, "foo"))
                                .build(),
                            defaultOp
                        )
                    )
                        .add(
                            new BooleanClause(
                                new SynonymQuery.Builder(TEXT_FIELD_NAME).addTerm(new Term(TEXT_FIELD_NAME, "bar"))
                                    .addTerm(new Term(TEXT_FIELD_NAME, "bar"))
                                    .build(),
                                defaultOp
                            )
                        )
                        .add(
                            new BooleanQuery.Builder().add(
                                new BooleanClause(new PrefixQuery(new Term(TEXT_FIELD_NAME, "foobar")), BooleanClause.Occur.SHOULD)
                            )
                                .add(new BooleanClause(new PrefixQuery(new Term(TEXT_FIELD_NAME, "foobar")), BooleanClause.Occur.SHOULD))
                                .build(),
                            defaultOp
                        )
                        .build(),
                    defaultOp
                )
                .add(
                    new BooleanClause(
                        new SynonymQuery.Builder(TEXT_FIELD_NAME).addTerm(new Term(TEXT_FIELD_NAME, "last"))
                            .addTerm(new Term(TEXT_FIELD_NAME, "last"))
                            .build(),
                        defaultOp
                    )
                )
                .build();
            assertThat(query, Matchers.equalTo(expectedQuery));
        }
    }

    public void testToQueryWithGraph() throws Exception {
        for (Operator op : Operator.values()) {
            BooleanClause.Occur defaultOp = op.toBooleanClauseOccur();
            QueryStringQueryParser queryParser = new QueryStringQueryParser(createShardContext(), TEXT_FIELD_NAME);
            queryParser.setAnalyzeWildcard(true);
            queryParser.setMultiTermRewriteMethod(MultiTermQuery.CONSTANT_SCORE_REWRITE);
            queryParser.setDefaultOperator(op.toQueryParserOperator());
            queryParser.setAnalyzeWildcard(true);
            queryParser.setMultiTermRewriteMethod(MultiTermQuery.CONSTANT_SCORE_REWRITE);
            queryParser.setDefaultOperator(op.toQueryParserOperator());
            queryParser.setForceAnalyzer(new MockSynonymAnalyzer());
            queryParser.setAutoGenerateMultiTermSynonymsPhraseQuery(false);

            // simple multi-term
            Query query = queryParser.parse("guinea pig");

            Query guineaPig = new BooleanQuery.Builder().add(new TermQuery(new Term(TEXT_FIELD_NAME, "guinea")), Occur.MUST)
                .add(new TermQuery(new Term(TEXT_FIELD_NAME, "pig")), Occur.MUST)
                .build();
            TermQuery cavy = new TermQuery(new Term(TEXT_FIELD_NAME, "cavy"));

            Query expectedQuery = new BooleanQuery.Builder().add(
                new BooleanQuery.Builder().add(guineaPig, Occur.SHOULD).add(cavy, Occur.SHOULD).build(),
                defaultOp
            ).build();
            assertThat(query, Matchers.equalTo(expectedQuery));

            queryParser.setAutoGenerateMultiTermSynonymsPhraseQuery(true);
            // simple multi-term with phrase query
            query = queryParser.parse("guinea pig");
            expectedQuery = new BooleanQuery.Builder().add(
                new BooleanQuery.Builder().add(
                    new PhraseQuery.Builder().add(new Term(TEXT_FIELD_NAME, "guinea")).add(new Term(TEXT_FIELD_NAME, "pig")).build(),
                    Occur.SHOULD
                ).add(new TermQuery(new Term(TEXT_FIELD_NAME, "cavy")), Occur.SHOULD).build(),
                defaultOp
            ).build();
            assertThat(query, Matchers.equalTo(expectedQuery));
            queryParser.setAutoGenerateMultiTermSynonymsPhraseQuery(false);

            // simple with additional tokens
            query = queryParser.parse("that guinea pig smells");
            expectedQuery = new BooleanQuery.Builder().add(new TermQuery(new Term(TEXT_FIELD_NAME, "that")), defaultOp)
                .add(new BooleanQuery.Builder().add(guineaPig, Occur.SHOULD).add(cavy, Occur.SHOULD).build(), defaultOp)
                .add(new TermQuery(new Term(TEXT_FIELD_NAME, "smells")), defaultOp)
                .build();
            assertThat(query, Matchers.equalTo(expectedQuery));

            // complex
            query = queryParser.parse("+that -(guinea pig) +smells");
            expectedQuery = new BooleanQuery.Builder().add(new TermQuery(new Term(TEXT_FIELD_NAME, "that")), Occur.MUST)
                .add(
                    new BooleanQuery.Builder().add(
                        new BooleanQuery.Builder().add(guineaPig, Occur.SHOULD).add(cavy, Occur.SHOULD).build(),
                        defaultOp
                    ).build(),
                    Occur.MUST_NOT
                )
                .add(new TermQuery(new Term(TEXT_FIELD_NAME, "smells")), Occur.MUST)
                .build();

            assertThat(query, Matchers.equalTo(expectedQuery));

            // no parent should cause guinea and pig to be treated as separate tokens
            query = queryParser.parse("+that -guinea pig +smells");
            expectedQuery = new BooleanQuery.Builder().add(new TermQuery(new Term(TEXT_FIELD_NAME, "that")), BooleanClause.Occur.MUST)
                .add(new TermQuery(new Term(TEXT_FIELD_NAME, "guinea")), BooleanClause.Occur.MUST_NOT)
                .add(new TermQuery(new Term(TEXT_FIELD_NAME, "pig")), defaultOp)
                .add(new TermQuery(new Term(TEXT_FIELD_NAME, "smells")), BooleanClause.Occur.MUST)
                .build();

            assertThat(query, Matchers.equalTo(expectedQuery));

            // span query
            query = queryParser.parse("\"that guinea pig smells\"");

            expectedQuery = new SpanNearQuery.Builder(TEXT_FIELD_NAME, true).addClause(new SpanTermQuery(new Term(TEXT_FIELD_NAME, "that")))
                .addClause(
                    new SpanOrQuery(
                        new SpanNearQuery.Builder(TEXT_FIELD_NAME, true).addClause(new SpanTermQuery(new Term(TEXT_FIELD_NAME, "guinea")))
                            .addClause(new SpanTermQuery(new Term(TEXT_FIELD_NAME, "pig")))
                            .build(),
                        new SpanTermQuery(new Term(TEXT_FIELD_NAME, "cavy"))
                    )
                )
                .addClause(new SpanTermQuery(new Term(TEXT_FIELD_NAME, "smells")))
                .build();
            assertThat(query, Matchers.equalTo(expectedQuery));

            // span query with slop
            query = queryParser.parse("\"that guinea pig smells\"~2");
            PhraseQuery pq1 = new PhraseQuery.Builder().add(new Term(TEXT_FIELD_NAME, "that"))
                .add(new Term(TEXT_FIELD_NAME, "guinea"))
                .add(new Term(TEXT_FIELD_NAME, "pig"))
                .add(new Term(TEXT_FIELD_NAME, "smells"))
                .setSlop(2)
                .build();
            PhraseQuery pq2 = new PhraseQuery.Builder().add(new Term(TEXT_FIELD_NAME, "that"))
                .add(new Term(TEXT_FIELD_NAME, "cavy"))
                .add(new Term(TEXT_FIELD_NAME, "smells"))
                .setSlop(2)
                .build();
            expectedQuery = new BooleanQuery.Builder().add(pq1, Occur.SHOULD).add(pq2, Occur.SHOULD).build();
            assertThat(query, Matchers.equalTo(expectedQuery));
        }
    }

    public void testToQueryRegExpQuery() throws Exception {
        Query query = queryStringQuery("/foo*bar/").defaultField(TEXT_FIELD_NAME).maxDeterminizedStates(5000).toQuery(createShardContext());
        assertThat(query, instanceOf(RegexpQuery.class));
        RegexpQuery regexpQuery = (RegexpQuery) query;
        assertTrue(regexpQuery.toString().contains("/foo*bar/"));
    }

    public void testToQueryRegExpQueryTooComplex() throws Exception {
        QueryStringQueryBuilder queryBuilder = queryStringQuery("/[ac]*a[ac]{50,200}/").defaultField(TEXT_FIELD_NAME);

        TooComplexToDeterminizeException e = expectThrows(
            TooComplexToDeterminizeException.class,
            () -> queryBuilder.toQuery(createShardContext())
        );
        assertThat(e.getMessage(), containsString("Determinizing [ac]*"));
        assertThat(e.getMessage(), containsString("would require more than 10000 effort"));
    }

    /**
     * Validates that {@code max_determinized_states} can be parsed and lowers the allowed number of determinized states.
     */
    public void testToQueryRegExpQueryMaxDeterminizedStatesParsing() throws Exception {
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        {
            builder.startObject("query_string");
            {
                builder.field("query", "/[ac]*a[ac]{1,10}/");
                builder.field("default_field", TEXT_FIELD_NAME);
                builder.field("max_determinized_states", 10);
            }
            builder.endObject();
        }
        builder.endObject();

        QueryBuilder queryBuilder = parseInnerQueryBuilder(createParser(builder));
        TooComplexToDeterminizeException e = expectThrows(
            TooComplexToDeterminizeException.class,
            () -> queryBuilder.toQuery(createShardContext())
        );
        assertThat(e.getMessage(), containsString("Determinizing [ac]*"));
        assertThat(e.getMessage(), containsString("would require more than 10 effort"));
    }

    public void testToQueryFuzzyQueryAutoFuzziness() throws Exception {
        for (int i = 0; i < 3; i++) {
            final int len;
            final int expectedEdits;
            switch (i) {
                case 0:
                    len = randomIntBetween(1, 2);
                    expectedEdits = 0;
                    break;

                case 1:
                    len = randomIntBetween(3, 5);
                    expectedEdits = 1;
                    break;

                default:
                    len = randomIntBetween(6, 20);
                    expectedEdits = 2;
                    break;
            }
            char[] bytes = new char[len];
            Arrays.fill(bytes, 'a');
            String queryString = new String(bytes);
            for (int j = 0; j < 2; j++) {
                Query query = queryStringQuery(queryString + (j == 0 ? "~" : "~auto")).defaultField(TEXT_FIELD_NAME)
                    .toQuery(createShardContext());
                assertThat(query, instanceOf(FuzzyQuery.class));
                FuzzyQuery fuzzyQuery = (FuzzyQuery) query;
                assertEquals(expectedEdits, fuzzyQuery.getMaxEdits());
            }
        }
    }

    public void testToQueryDateWithTimeZone() throws Exception {
        QueryStringQueryBuilder qsq = queryStringQuery(DATE_FIELD_NAME + ":1970-01-01");
        QueryShardContext context = createShardContext();
        Query query = qsq.toQuery(context);
        assumeThat(
            "Using Approximate Range Query as default",
            FeatureFlags.isEnabled(FeatureFlags.APPROXIMATE_POINT_RANGE_QUERY),
            is(true)
        );
        assertThat(query, instanceOf(ApproximateScoreQuery.class));
        long lower = 0; // 1970-01-01T00:00:00.999 UTC
        long upper = 86399999;  // 1970-01-01T23:59:59.999 UTC
        assertEquals(calculateExpectedDateQuery(lower, upper), query);
        int msPerHour = 3600000;
        assertEquals(calculateExpectedDateQuery(lower - msPerHour, upper - msPerHour), qsq.timeZone("+01:00").toQuery(context));
        assertEquals(calculateExpectedDateQuery(lower + msPerHour, upper + msPerHour), qsq.timeZone("-01:00").toQuery(context));
    }

    private ApproximateScoreQuery calculateExpectedDateQuery(long lower, long upper) {
        return new ApproximateScoreQuery(
            new IndexOrDocValuesQuery(
                LongPoint.newRangeQuery(DATE_FIELD_NAME, lower, upper),
                SortedNumericDocValuesField.newSlowRangeQuery(DATE_FIELD_NAME, lower, upper)
            ),
            new ApproximatePointRangeQuery(
                DATE_FIELD_NAME,
                pack(new long[] { lower }).bytes,
                pack(new long[] { upper }).bytes,
                new long[] { lower }.length
            ) {
                @Override
                protected String toString(int dimension, byte[] value) {
                    return Long.toString(LongPoint.decodeDimension(value, 0));
                }
            }
        );
    }

    public void testFuzzyNumeric() throws Exception {
        QueryStringQueryBuilder query = queryStringQuery("12~0.2").defaultField(INT_FIELD_NAME);
        QueryShardContext context = createShardContext();
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> query.toQuery(context));
        assertEquals(
            "Can only use fuzzy queries on keyword and text fields - not on [mapped_int] which is of type [integer]",
            e.getMessage()
        );
        query.lenient(true);
        query.toQuery(context); // no exception
    }

    public void testDefaultFuzziness() {
        QueryStringQueryBuilder queryStringQueryBuilder = new QueryStringQueryBuilder(TEXT_FIELD_NAME).fuzziness(null);
        assertNull(queryStringQueryBuilder.fuzziness());
    }

    public void testPrefixNumeric() throws Exception {
        QueryStringQueryBuilder query = queryStringQuery("12*").defaultField(INT_FIELD_NAME);
        QueryShardContext context = createShardContext();
        QueryShardException e = expectThrows(QueryShardException.class, () -> query.toQuery(context));
        assertEquals(
            "Can only use prefix queries on keyword and text fields - not on [mapped_int] which is of type [integer]",
            e.getMessage()
        );
        query.lenient(true);
        query.toQuery(context); // no exception
    }

    public void testExactGeo() throws Exception {
        QueryStringQueryBuilder query = queryStringQuery("2,3").defaultField(GEO_POINT_FIELD_NAME);
        QueryShardContext context = createShardContext();
        QueryShardException e = expectThrows(QueryShardException.class, () -> query.toQuery(context));
        assertEquals(
            "Geometry fields do not support exact searching, use dedicated geometry queries instead: " + "[mapped_geo_point]",
            e.getMessage()
        );
        query.lenient(true);
        query.toQuery(context); // no exception
    }

    public void testTimezone() throws Exception {
        String queryAsString = "{\n"
            + "    \"query_string\":{\n"
            + "        \"time_zone\":\"Europe/Paris\",\n"
            + "        \"query\":\""
            + DATE_FIELD_NAME
            + ":[2012 TO 2014]\"\n"
            + "    }\n"
            + "}";
        QueryBuilder queryBuilder = parseQuery(queryAsString);
        assertThat(queryBuilder, instanceOf(QueryStringQueryBuilder.class));
        QueryStringQueryBuilder queryStringQueryBuilder = (QueryStringQueryBuilder) queryBuilder;
        assertThat(queryStringQueryBuilder.timeZone(), equalTo(ZoneId.of("Europe/Paris")));

        String invalidQueryAsString = "{\n"
            + "    \"query_string\":{\n"
            + "        \"time_zone\":\"This timezone does not exist\",\n"
            + "        \"query\":\""
            + DATE_FIELD_NAME
            + ":[2012 TO 2014]\"\n"
            + "    }\n"
            + "}";
        expectThrows(DateTimeException.class, () -> parseQuery(invalidQueryAsString));
    }

    public void testToQueryBooleanQueryMultipleBoosts() throws Exception {
        int numBoosts = randomIntBetween(2, 10);
        float[] boosts = new float[numBoosts + 1];
        String queryStringPrefix = "";
        String queryStringSuffix = "";
        for (int i = 0; i < boosts.length - 1; i++) {
            float boost = 2.0f / randomIntBetween(3, 20);
            boosts[i] = boost;
            queryStringPrefix += "(";
            queryStringSuffix += ")^" + boost;
        }
        String queryString = queryStringPrefix + "foo bar" + queryStringSuffix;

        float mainBoost = 2.0f / randomIntBetween(3, 20);
        boosts[boosts.length - 1] = mainBoost;
        QueryStringQueryBuilder queryStringQueryBuilder = new QueryStringQueryBuilder(queryString).field(TEXT_FIELD_NAME)
            .minimumShouldMatch("2")
            .boost(mainBoost);
        Query query = queryStringQueryBuilder.toQuery(createShardContext());

        for (int i = boosts.length - 1; i >= 0; i--) {
            assertThat(query, instanceOf(BoostQuery.class));
            BoostQuery boostQuery = (BoostQuery) query;
            assertThat(boostQuery.getBoost(), equalTo(boosts[i]));
            query = boostQuery.getQuery();
        }

        assertThat(query, instanceOf(BooleanQuery.class));
        BooleanQuery booleanQuery = (BooleanQuery) query;
        assertThat(booleanQuery.getMinimumNumberShouldMatch(), equalTo(2));
        assertThat(booleanQuery.clauses().get(0).getOccur(), equalTo(BooleanClause.Occur.SHOULD));
        assertThat(booleanQuery.clauses().get(0).getQuery(), equalTo(new TermQuery(new Term(TEXT_FIELD_NAME, "foo"))));
        assertThat(booleanQuery.clauses().get(1).getOccur(), equalTo(BooleanClause.Occur.SHOULD));
        assertThat(booleanQuery.clauses().get(1).getQuery(), equalTo(new TermQuery(new Term(TEXT_FIELD_NAME, "bar"))));
    }

    public void testToQueryPhraseQueryBoostAndSlop() throws IOException {
        QueryStringQueryBuilder queryStringQueryBuilder = new QueryStringQueryBuilder("\"test phrase\"~2").field(TEXT_FIELD_NAME, 5f);
        Query query = queryStringQueryBuilder.toQuery(createShardContext());
        assertThat(query, instanceOf(BoostQuery.class));
        BoostQuery boostQuery = (BoostQuery) query;
        assertThat(boostQuery.getBoost(), equalTo(5f));
        assertThat(boostQuery.getQuery(), instanceOf(PhraseQuery.class));
        PhraseQuery phraseQuery = (PhraseQuery) boostQuery.getQuery();
        assertThat(phraseQuery.getSlop(), Matchers.equalTo(2));
        assertThat(phraseQuery.getTerms().length, equalTo(2));
    }

    public void testToQueryWildcardNonExistingFields() throws IOException {
        QueryStringQueryBuilder queryStringQueryBuilder = new QueryStringQueryBuilder("foo bar").field("invalid*");
        Query query = queryStringQueryBuilder.toQuery(createShardContext());

        Query expectedQuery = new MatchNoDocsQuery("empty fields");
        assertThat(expectedQuery, equalTo(query));

        queryStringQueryBuilder = new QueryStringQueryBuilder(TEXT_FIELD_NAME + ":foo bar").field("invalid*");
        query = queryStringQueryBuilder.toQuery(createShardContext());
        expectedQuery = new BooleanQuery.Builder().add(new TermQuery(new Term(TEXT_FIELD_NAME, "foo")), Occur.SHOULD)
            .add(new MatchNoDocsQuery("empty fields"), Occur.SHOULD)
            .build();
        assertThat(expectedQuery, equalTo(query));
    }

    public void testToQueryTextParsing() throws IOException {
        {
            QueryStringQueryBuilder queryBuilder = new QueryStringQueryBuilder("foo bar").field(TEXT_FIELD_NAME).field(KEYWORD_FIELD_NAME);
            Query query = queryBuilder.toQuery(createShardContext());
            BooleanQuery bq1 = new BooleanQuery.Builder().add(
                new BooleanClause(new TermQuery(new Term(TEXT_FIELD_NAME, "foo")), BooleanClause.Occur.SHOULD)
            ).add(new BooleanClause(new TermQuery(new Term(TEXT_FIELD_NAME, "bar")), BooleanClause.Occur.SHOULD)).build();
            List<Query> disjuncts = new ArrayList<>();
            disjuncts.add(bq1);
            disjuncts.add(new TermQuery(new Term(KEYWORD_FIELD_NAME, "foo bar")));
            DisjunctionMaxQuery expectedQuery = new DisjunctionMaxQuery(disjuncts, 0.0f);
            assertThat(query, equalTo(expectedQuery));
        }

        // type=phrase
        {
            QueryStringQueryBuilder queryBuilder = new QueryStringQueryBuilder("foo bar").field(TEXT_FIELD_NAME).field(KEYWORD_FIELD_NAME);
            queryBuilder.type(MultiMatchQueryBuilder.Type.PHRASE);
            Query query = queryBuilder.toQuery(createShardContext());

            List<Query> disjuncts = new ArrayList<>();
            PhraseQuery pq = new PhraseQuery.Builder().add(new Term(TEXT_FIELD_NAME, "foo")).add(new Term(TEXT_FIELD_NAME, "bar")).build();
            disjuncts.add(pq);
            disjuncts.add(new TermQuery(new Term(KEYWORD_FIELD_NAME, "foo bar")));
            DisjunctionMaxQuery expectedQuery = new DisjunctionMaxQuery(disjuncts, 0.0f);
            assertThat(query, equalTo(expectedQuery));
        }

        {
            QueryStringQueryBuilder queryBuilder = new QueryStringQueryBuilder("mapped_string:other foo bar").field(TEXT_FIELD_NAME)
                .field(KEYWORD_FIELD_NAME);
            Query query = queryBuilder.toQuery(createShardContext());
            BooleanQuery bq1 = new BooleanQuery.Builder().add(
                new BooleanClause(new TermQuery(new Term(TEXT_FIELD_NAME, "foo")), BooleanClause.Occur.SHOULD)
            ).add(new BooleanClause(new TermQuery(new Term(TEXT_FIELD_NAME, "bar")), BooleanClause.Occur.SHOULD)).build();
            List<Query> disjuncts = new ArrayList<>();
            disjuncts.add(bq1);
            disjuncts.add(new TermQuery(new Term(KEYWORD_FIELD_NAME, "foo bar")));
            DisjunctionMaxQuery disjunctionMaxQuery = new DisjunctionMaxQuery(disjuncts, 0.0f);
            BooleanQuery expectedQuery = new BooleanQuery.Builder().add(disjunctionMaxQuery, BooleanClause.Occur.SHOULD)
                .add(new TermQuery(new Term(TEXT_FIELD_NAME, "other")), BooleanClause.Occur.SHOULD)
                .build();
            assertThat(query, equalTo(expectedQuery));
        }

        {
            QueryStringQueryBuilder queryBuilder = new QueryStringQueryBuilder("foo OR bar").field(TEXT_FIELD_NAME)
                .field(KEYWORD_FIELD_NAME);
            Query query = queryBuilder.toQuery(createShardContext());

            List<Query> disjuncts1 = new ArrayList<>();
            disjuncts1.add(new TermQuery(new Term(TEXT_FIELD_NAME, "foo")));
            disjuncts1.add(new TermQuery(new Term(KEYWORD_FIELD_NAME, "foo")));
            DisjunctionMaxQuery maxQuery1 = new DisjunctionMaxQuery(disjuncts1, 0.0f);

            List<Query> disjuncts2 = new ArrayList<>();
            disjuncts2.add(new TermQuery(new Term(TEXT_FIELD_NAME, "bar")));
            disjuncts2.add(new TermQuery(new Term(KEYWORD_FIELD_NAME, "bar")));
            DisjunctionMaxQuery maxQuery2 = new DisjunctionMaxQuery(disjuncts2, 0.0f);

            BooleanQuery expectedQuery = new BooleanQuery.Builder().add(new BooleanClause(maxQuery1, BooleanClause.Occur.SHOULD))
                .add(new BooleanClause(maxQuery2, BooleanClause.Occur.SHOULD))
                .build();
            assertThat(query, equalTo(expectedQuery));
        }

        // non-prefix queries do not work with range queries simple syntax
        {
            // throws an exception when lenient is set to false
            QueryStringQueryBuilder queryBuilder = new QueryStringQueryBuilder(">10 foo").field(INT_FIELD_NAME);
            IllegalArgumentException exc = expectThrows(IllegalArgumentException.class, () -> queryBuilder.toQuery(createShardContext()));
            assertThat(exc.getMessage(), equalTo("For input string: \">10 foo\""));
        }
    }

    public void testExistsFieldQuery() throws Exception {
        QueryShardContext context = createShardContext();
        QueryStringQueryBuilder queryBuilder = new QueryStringQueryBuilder(TEXT_FIELD_NAME + ":*");
        Query query = queryBuilder.toQuery(context);
        if ((context.getMapperService().fieldType(TEXT_FIELD_NAME).getTextSearchInfo().hasNorms())) {
            assertThat(query, equalTo(new ConstantScoreQuery(new NormsFieldExistsQuery(TEXT_FIELD_NAME))));
        } else {
            assertThat(query, equalTo(new ConstantScoreQuery(new TermQuery(new Term("_field_names", TEXT_FIELD_NAME)))));
        }

        for (boolean quoted : new boolean[] { true, false }) {
            String value = (quoted ? "\"" : "") + TEXT_FIELD_NAME + (quoted ? "\"" : "");
            queryBuilder = new QueryStringQueryBuilder("_exists_:" + value);
            query = queryBuilder.toQuery(context);
            if ((context.getMapperService().fieldType(TEXT_FIELD_NAME).getTextSearchInfo().hasNorms())) {
                assertThat(query, equalTo(new ConstantScoreQuery(new NormsFieldExistsQuery(TEXT_FIELD_NAME))));
            } else {
                assertThat(query, equalTo(new ConstantScoreQuery(new TermQuery(new Term("_field_names", TEXT_FIELD_NAME)))));
            }
        }
        QueryShardContext contextNoType = createShardContextWithNoType();
        query = queryBuilder.toQuery(contextNoType);
        assertThat(query, equalTo(new MatchNoDocsQuery()));

        queryBuilder = new QueryStringQueryBuilder("*:*");
        query = queryBuilder.toQuery(context);
        Query expected = new MatchAllDocsQuery();
        assertThat(query, equalTo(expected));

        queryBuilder = new QueryStringQueryBuilder("*");
        query = queryBuilder.toQuery(context);
        expected = new MatchAllDocsQuery();
        assertThat(query, equalTo(expected));
    }

    public void testDisabledFieldNamesField() throws Exception {
        QueryShardContext context = createShardContext();
        context.getMapperService()
            .merge(
                "_doc",
                new CompressedXContent(PutMappingRequest.simpleMapping("foo", "type=text", "_field_names", "enabled=false").toString()),
                MapperService.MergeReason.MAPPING_UPDATE
            );

        try {
            QueryStringQueryBuilder queryBuilder = new QueryStringQueryBuilder("foo:*");
            Query query = queryBuilder.toQuery(context);
            Query expected = new WildcardQuery(new Term("foo", "*"));
            assertThat(query, equalTo(expected));
        } finally {
            // restore mappings as they were before
            context.getMapperService()
                .merge(
                    "_doc",
                    new CompressedXContent(PutMappingRequest.simpleMapping("foo", "type=text", "_field_names", "enabled=true").toString()),
                    MapperService.MergeReason.MAPPING_UPDATE
                );
        }
        assertWarnings(FieldNamesFieldMapper.ENABLED_DEPRECATION_MESSAGE);
    }

    public void testFromJson() throws IOException {
        String json = "{\n"
            + "  \"query_string\" : {\n"
            + "    \"query\" : \"this AND that OR thus\",\n"
            + "    \"default_field\" : \"content\",\n"
            + "    \"fields\" : [ ],\n"
            + "    \"type\" : \"best_fields\",\n"
            + "    \"tie_breaker\" : 0.0,\n"
            + "    \"default_operator\" : \"or\",\n"
            + "    \"max_determinized_states\" : 10000,\n"
            + "    \"enable_position_increments\" : true,\n"
            + "    \"fuzziness\" : \"AUTO\",\n"
            + "    \"fuzzy_prefix_length\" : 0,\n"
            + "    \"fuzzy_max_expansions\" : 50,\n"
            + "    \"phrase_slop\" : 0,\n"
            + "    \"escape\" : false,\n"
            + "    \"auto_generate_synonyms_phrase_query\" : true,\n"
            + "    \"fuzzy_transpositions\" : false,\n"
            + "    \"boost\" : 1.0\n"
            + "  }\n"
            + "}";

        QueryStringQueryBuilder parsed = (QueryStringQueryBuilder) parseQuery(json);
        checkGeneratedJson(json, parsed);

        assertEquals(json, "this AND that OR thus", parsed.queryString());
        assertEquals(json, "content", parsed.defaultField());
        assertEquals(json, false, parsed.fuzzyTranspositions());
    }

    public void testExpandedTerms() throws Exception {
        // Prefix
        Query query = new QueryStringQueryBuilder("aBc*").field(TEXT_FIELD_NAME).analyzer("whitespace").toQuery(createShardContext());
        assertEquals(new PrefixQuery(new Term(TEXT_FIELD_NAME, "aBc"), MultiTermQuery.CONSTANT_SCORE_REWRITE), query);
        query = new QueryStringQueryBuilder("aBc*").field(TEXT_FIELD_NAME).analyzer("standard").toQuery(createShardContext());
        assertEquals(new PrefixQuery(new Term(TEXT_FIELD_NAME, "abc"), MultiTermQuery.CONSTANT_SCORE_REWRITE), query);

        // Wildcard
        query = new QueryStringQueryBuilder("aBc*D").field(TEXT_FIELD_NAME).analyzer("whitespace").toQuery(createShardContext());
        assertEquals(
            new WildcardQuery(
                new Term(TEXT_FIELD_NAME, "aBc*D"),
                Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
                MultiTermQuery.CONSTANT_SCORE_REWRITE
            ),
            query
        );
        query = new QueryStringQueryBuilder("aBc*D").field(TEXT_FIELD_NAME).analyzer("standard").toQuery(createShardContext());
        assertEquals(
            new WildcardQuery(
                new Term(TEXT_FIELD_NAME, "abc*d"),
                Operations.DEFAULT_DETERMINIZE_WORK_LIMIT,
                MultiTermQuery.CONSTANT_SCORE_REWRITE
            ),
            query
        );

        // Fuzzy
        query = new QueryStringQueryBuilder("aBc~1").field(TEXT_FIELD_NAME).analyzer("whitespace").toQuery(createShardContext());
        FuzzyQuery fuzzyQuery = (FuzzyQuery) query;
        assertEquals(new Term(TEXT_FIELD_NAME, "aBc"), fuzzyQuery.getTerm());
        query = new QueryStringQueryBuilder("aBc~1").field(TEXT_FIELD_NAME).analyzer("standard").toQuery(createShardContext());
        fuzzyQuery = (FuzzyQuery) query;
        assertEquals(new Term(TEXT_FIELD_NAME, "abc"), fuzzyQuery.getTerm());

        // Range
        query = new QueryStringQueryBuilder("[aBc TO BcD]").field(TEXT_FIELD_NAME).analyzer("whitespace").toQuery(createShardContext());
        assertEquals(new TermRangeQuery(TEXT_FIELD_NAME, new BytesRef("aBc"), new BytesRef("BcD"), true, true), query);
        query = new QueryStringQueryBuilder("[aBc TO BcD]").field(TEXT_FIELD_NAME).analyzer("standard").toQuery(createShardContext());
        assertEquals(new TermRangeQuery(TEXT_FIELD_NAME, new BytesRef("abc"), new BytesRef("bcd"), true, true), query);
    }

    public void testDefaultFieldsWithFields() throws IOException {
        QueryShardContext context = createShardContext();
        QueryStringQueryBuilder builder = new QueryStringQueryBuilder("aBc*").field("field").defaultField("*");
        QueryValidationException e = expectThrows(QueryValidationException.class, () -> builder.toQuery(context));
        assertThat(e.getMessage(), containsString("cannot use [fields] parameter in conjunction with [default_field]"));
    }

    public void testLenientRewriteToMatchNoDocs() throws IOException {
        // Term
        Query query = new QueryStringQueryBuilder("hello").field(INT_FIELD_NAME).lenient(true).toQuery(createShardContext());
        assertEquals(new MatchNoDocsQuery(""), query);

        // prefix
        query = new QueryStringQueryBuilder("hello*").field(INT_FIELD_NAME).lenient(true).toQuery(createShardContext());
        assertEquals(new MatchNoDocsQuery(""), query);

        // Fuzzy
        query = new QueryStringQueryBuilder("hello~2").field(INT_FIELD_NAME).lenient(true).toQuery(createShardContext());
        assertEquals(new MatchNoDocsQuery(""), query);
    }

    public void testUnmappedFieldRewriteToMatchNoDocs() throws IOException {
        // Default unmapped field
        Query query = new QueryStringQueryBuilder("hello").field("unmapped_field").lenient(true).toQuery(createShardContext());
        assertEquals(new MatchNoDocsQuery(), query);

        // Unmapped prefix field
        query = new QueryStringQueryBuilder("unmapped_field:hello").lenient(true).toQuery(createShardContext());
        assertEquals(new MatchNoDocsQuery(), query);

        // Unmapped fields
        query = new QueryStringQueryBuilder("hello").lenient(true)
            .field("unmapped_field")
            .field("another_field")
            .toQuery(createShardContext());
        assertEquals(new MatchNoDocsQuery(), query);

        // Multi block
        query = new QueryStringQueryBuilder("first unmapped:second").field(TEXT_FIELD_NAME)
            .field("unmapped")
            .field("another_unmapped")
            .defaultOperator(Operator.AND)
            .toQuery(createShardContext());
        BooleanQuery expected = new BooleanQuery.Builder().add(new TermQuery(new Term(TEXT_FIELD_NAME, "first")), BooleanClause.Occur.MUST)
            .add(new MatchNoDocsQuery(), BooleanClause.Occur.MUST)
            .build();
        assertEquals(expected, query);

        query = new SimpleQueryStringBuilder("first unknown:second").field("unmapped")
            .field("another_unmapped")
            .defaultOperator(Operator.AND)
            .toQuery(createShardContext());
        expected = new BooleanQuery.Builder().add(new MatchNoDocsQuery(), BooleanClause.Occur.MUST)
            .add(new MatchNoDocsQuery(), BooleanClause.Occur.MUST)
            .build();
        assertEquals(expected, query);

    }

    public void testDefaultField() throws Exception {
        QueryShardContext context = createShardContext();
        // default value `*` sets leniency to true
        Query query = new QueryStringQueryBuilder("hello").toQuery(context);
        assertQueryWithAllFieldsWildcard(query);

        try {
            // `*` is in the list of the default_field => leniency set to true
            context.getIndexSettings()
                .updateIndexMetadata(
                    newIndexMeta(
                        "index",
                        context.getIndexSettings().getSettings(),
                        Settings.builder().putList("index.query.default_field", TEXT_FIELD_NAME, "*", KEYWORD_FIELD_NAME).build()
                    )
                );
            query = new QueryStringQueryBuilder("hello").toQuery(context);
            assertQueryWithAllFieldsWildcard(query);

            context.getIndexSettings()
                .updateIndexMetadata(
                    newIndexMeta(
                        "index",
                        context.getIndexSettings().getSettings(),
                        Settings.builder().putList("index.query.default_field", TEXT_FIELD_NAME, KEYWORD_FIELD_NAME + "^5").build()
                    )
                );
            query = new QueryStringQueryBuilder("hello").toQuery(context);
            Query expected = new DisjunctionMaxQuery(
                Arrays.asList(
                    new TermQuery(new Term(TEXT_FIELD_NAME, "hello")),
                    new BoostQuery(new TermQuery(new Term(KEYWORD_FIELD_NAME, "hello")), 5.0f)
                ),
                0.0f
            );
            assertEquals(expected, query);
        } finally {
            // Reset the default value
            context.getIndexSettings()
                .updateIndexMetadata(
                    newIndexMeta(
                        "index",
                        context.getIndexSettings().getSettings(),
                        Settings.builder().putList("index.query.default_field", "*").build()
                    )
                );
        }
    }

    public void testAllFieldsWildcard() throws Exception {
        QueryShardContext context = createShardContext();
        Query query = new QueryStringQueryBuilder("hello").field("*").toQuery(context);
        assertQueryWithAllFieldsWildcard(query);

        query = new QueryStringQueryBuilder("hello").field(TEXT_FIELD_NAME).field("*").field(KEYWORD_FIELD_NAME).toQuery(context);
        assertQueryWithAllFieldsWildcard(query);
    }

    /**
     * the quote analyzer should overwrite any other forced analyzer in quoted parts of the query
     */
    public void testQuoteAnalyzer() throws Exception {
        // Prefix
        Query query = new QueryStringQueryBuilder("ONE \"TWO THREE\"").field(TEXT_FIELD_NAME)
            .analyzer("whitespace")
            .quoteAnalyzer("simple")
            .toQuery(createShardContext());
        Query expectedQuery = new BooleanQuery.Builder().add(
            new BooleanClause(new TermQuery(new Term(TEXT_FIELD_NAME, "ONE")), Occur.SHOULD)
        )
            .add(
                new BooleanClause(
                    new PhraseQuery.Builder().add(new Term(TEXT_FIELD_NAME, "two"), 0).add(new Term(TEXT_FIELD_NAME, "three"), 1).build(),
                    Occur.SHOULD
                )
            )
            .build();
        assertEquals(expectedQuery, query);
    }

    public void testQuoteFieldSuffix() throws IOException {
        QueryShardContext context = createShardContext();
        assertEquals(
            new TermQuery(new Term(TEXT_FIELD_NAME, "bar")),
            new QueryStringQueryBuilder("bar").quoteFieldSuffix("_2").field(TEXT_FIELD_NAME).doToQuery(context)
        );
        assertEquals(
            new TermQuery(new Term(KEYWORD_FIELD_NAME, "bar")),
            new QueryStringQueryBuilder("\"bar\"").quoteFieldSuffix("_2").field(TEXT_FIELD_NAME).doToQuery(context)
        );

        // Now check what happens if the quote field does not exist
        assertEquals(
            new TermQuery(new Term(TEXT_FIELD_NAME, "bar")),
            new QueryStringQueryBuilder("bar").quoteFieldSuffix(".quote").field(TEXT_FIELD_NAME).doToQuery(context)
        );
        assertEquals(
            new TermQuery(new Term(TEXT_FIELD_NAME, "bar")),
            new QueryStringQueryBuilder("\"bar\"").quoteFieldSuffix(".quote").field(TEXT_FIELD_NAME).doToQuery(context)
        );
    }

    public void testToFuzzyQuery() throws Exception {
        Query query = new QueryStringQueryBuilder("text~2").field(TEXT_FIELD_NAME)
            .fuzzyPrefixLength(2)
            .fuzzyMaxExpansions(5)
            .fuzzyTranspositions(false)
            .toQuery(createShardContext());
        FuzzyQuery expected = new FuzzyQuery(new Term(TEXT_FIELD_NAME, "text"), 2, 2, 5, false);
        assertEquals(expected, query);
    }

    public void testWithStopWords() throws Exception {
        Query query = new QueryStringQueryBuilder("the quick fox").field(TEXT_FIELD_NAME).analyzer("stop").toQuery(createShardContext());
        Query expected = new BooleanQuery.Builder().add(new TermQuery(new Term(TEXT_FIELD_NAME, "quick")), BooleanClause.Occur.SHOULD)
            .add(new TermQuery(new Term(TEXT_FIELD_NAME, "fox")), BooleanClause.Occur.SHOULD)
            .build();
        assertEquals(expected, query);

        query = new QueryStringQueryBuilder("the quick fox").field(TEXT_FIELD_NAME)
            .field(KEYWORD_FIELD_NAME)
            .analyzer("stop")
            .toQuery(createShardContext());
        expected = new DisjunctionMaxQuery(
            Arrays.asList(
                new BooleanQuery.Builder().add(new TermQuery(new Term(TEXT_FIELD_NAME, "quick")), Occur.SHOULD)
                    .add(new TermQuery(new Term(TEXT_FIELD_NAME, "fox")), Occur.SHOULD)
                    .build(),
                new BooleanQuery.Builder().add(new TermQuery(new Term(KEYWORD_FIELD_NAME, "quick")), Occur.SHOULD)
                    .add(new TermQuery(new Term(KEYWORD_FIELD_NAME, "fox")), Occur.SHOULD)
                    .build()
            ),
            0f
        );
        assertEquals(expected, query);

        query = new QueryStringQueryBuilder("the").field(TEXT_FIELD_NAME)
            .field(KEYWORD_FIELD_NAME)
            .analyzer("stop")
            .toQuery(createShardContext());
        assertEquals(new BooleanQuery.Builder().build(), query);

        query = new BoolQueryBuilder().should(new QueryStringQueryBuilder("the").field(TEXT_FIELD_NAME).analyzer("stop"))
            .toQuery(createShardContext());
        expected = new BooleanQuery.Builder().add(new BooleanQuery.Builder().build(), BooleanClause.Occur.SHOULD).build();
        assertEquals(expected, query);

        query = new BoolQueryBuilder().should(
            new QueryStringQueryBuilder("the").field(TEXT_FIELD_NAME).field(KEYWORD_FIELD_NAME).analyzer("stop")
        ).toQuery(createShardContext());
        assertEquals(expected, query);
    }

    public void testEnablePositionIncrement() throws Exception {
        Query query = new QueryStringQueryBuilder("\"quick the fox\"").field(TEXT_FIELD_NAME)
            .analyzer("stop")
            .enablePositionIncrements(false)
            .toQuery(createShardContext());
        PhraseQuery expected = new PhraseQuery.Builder().add(new Term(TEXT_FIELD_NAME, "quick"))
            .add(new Term(TEXT_FIELD_NAME, "fox"))
            .build();
        assertEquals(expected, query);
    }

    public void testWithPrefixStopWords() throws Exception {
        Query query = new QueryStringQueryBuilder("the* quick fox").field(TEXT_FIELD_NAME).analyzer("stop").toQuery(createShardContext());
        BooleanQuery expected = new BooleanQuery.Builder().add(
            new PrefixQuery(new Term(TEXT_FIELD_NAME, "the"), MultiTermQuery.CONSTANT_SCORE_REWRITE),
            Occur.SHOULD
        )
            .add(new TermQuery(new Term(TEXT_FIELD_NAME, "quick")), Occur.SHOULD)
            .add(new TermQuery(new Term(TEXT_FIELD_NAME, "fox")), Occur.SHOULD)
            .build();
        assertEquals(expected, query);
    }

    public void testCrossFields() throws Exception {
        final QueryShardContext context = createShardContext();
        context.getIndexSettings()
            .updateIndexMetadata(
                newIndexMeta(
                    "index",
                    context.getIndexSettings().getSettings(),
                    Settings.builder().putList("index.query.default_field", TEXT_FIELD_NAME, KEYWORD_FIELD_NAME).build()
                )
            );
        try {
            Term[] blendedTerms = new Term[2];
            blendedTerms[0] = new Term(TEXT_FIELD_NAME, "foo");
            blendedTerms[1] = new Term(KEYWORD_FIELD_NAME, "foo");

            Query query = new QueryStringQueryBuilder("foo").analyzer("whitespace")
                .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                .toQuery(createShardContext());
            Query expected = BlendedTermQuery.dismaxBlendedQuery(blendedTerms, 1.0f);
            assertEquals(expected, query);

            query = new QueryStringQueryBuilder("foo mapped_string:10").analyzer("whitespace")
                .type(MultiMatchQueryBuilder.Type.CROSS_FIELDS)
                .toQuery(createShardContext());
            expected = new BooleanQuery.Builder().add(BlendedTermQuery.dismaxBlendedQuery(blendedTerms, 1.0f), Occur.SHOULD)
                .add(new TermQuery(new Term(TEXT_FIELD_NAME, "10")), Occur.SHOULD)
                .build();
            assertEquals(expected, query);
        } finally {
            // Reset the default value
            context.getIndexSettings()
                .updateIndexMetadata(
                    newIndexMeta(
                        "index",
                        context.getIndexSettings().getSettings(),
                        Settings.builder().putList("index.query.default_field", "*").build()
                    )
                );
        }
    }

    public void testPhraseSlop() throws Exception {
        Query query = new QueryStringQueryBuilder("quick fox").field(TEXT_FIELD_NAME)
            .type(MultiMatchQueryBuilder.Type.PHRASE)
            .toQuery(createShardContext());

        PhraseQuery expected = new PhraseQuery.Builder().add(new Term(TEXT_FIELD_NAME, "quick"))
            .add(new Term(TEXT_FIELD_NAME, "fox"))
            .build();
        assertEquals(expected, query);

        query = new QueryStringQueryBuilder("quick fox").field(TEXT_FIELD_NAME)
            .type(MultiMatchQueryBuilder.Type.PHRASE)
            .phraseSlop(2)
            .toQuery(createShardContext());

        expected = new PhraseQuery.Builder().add(new Term(TEXT_FIELD_NAME, "quick"))
            .add(new Term(TEXT_FIELD_NAME, "fox"))
            .setSlop(2)
            .build();
        assertEquals(expected, query);

        query = new QueryStringQueryBuilder("\"quick fox\"").field(TEXT_FIELD_NAME).phraseSlop(2).toQuery(createShardContext());
        assertEquals(expected, query);

        query = new QueryStringQueryBuilder("\"quick fox\"~2").field(TEXT_FIELD_NAME).phraseSlop(10).toQuery(createShardContext());
        assertEquals(expected, query);
    }

    public void testAnalyzedPrefix() throws Exception {
        Query query = new QueryStringQueryBuilder("quick* @&*").field(TEXT_FIELD_NAME)
            .analyzer("standard")
            .analyzeWildcard(true)
            .toQuery(createShardContext());
        Query expected = new PrefixQuery(new Term(TEXT_FIELD_NAME, "quick"), MultiTermQuery.CONSTANT_SCORE_REWRITE);
        assertEquals(expected, query);
    }

    public void testNegativeFieldBoost() {
        IllegalArgumentException exc = expectThrows(
            IllegalArgumentException.class,
            () -> new QueryStringQueryBuilder("the quick fox").field(TEXT_FIELD_NAME, -1.0f)
                .field(KEYWORD_FIELD_NAME)
                .toQuery(createShardContext())
        );
        assertThat(exc.getMessage(), CoreMatchers.containsString("negative [boost]"));
    }

    public void testMergeBoosts() throws IOException {
        Query query = new QueryStringQueryBuilder("first").type(MultiMatchQueryBuilder.Type.MOST_FIELDS)
            .field(TEXT_FIELD_NAME, 0.3f)
            .field(TEXT_FIELD_NAME.substring(0, TEXT_FIELD_NAME.length() - 2) + "*", 0.5f)
            .toQuery(createShardContext());
        List<Query> terms = new ArrayList<>();
        terms.add(new BoostQuery(new TermQuery(new Term(TEXT_FIELD_NAME, "first")), 0.075f));
        terms.add(new BoostQuery(new TermQuery(new Term(KEYWORD_FIELD_NAME, "first")), 0.5f));
        Query expected = new DisjunctionMaxQuery(terms, 1.0f);
        assertEquals(expected, query);
    }

    private static IndexMetadata newIndexMeta(String name, Settings oldIndexSettings, Settings indexSettings) {
        Settings build = Settings.builder().put(oldIndexSettings).put(indexSettings).build();
        return IndexMetadata.builder(name).settings(build).build();
    }

    private void assertQueryWithAllFieldsWildcard(Query query) {
        assertEquals(DisjunctionMaxQuery.class, query.getClass());
        DisjunctionMaxQuery disjunctionMaxQuery = (DisjunctionMaxQuery) query;
        int noMatchNoDocsQueries = 0;
        for (Query q : disjunctionMaxQuery.getDisjuncts()) {
            if (q.getClass() == MatchNoDocsQuery.class) {
                noMatchNoDocsQueries++;
            }
        }
        assertEquals(9, noMatchNoDocsQueries);
        assertThat(
            disjunctionMaxQuery.getDisjuncts(),
            hasItems(new TermQuery(new Term(TEXT_FIELD_NAME, "hello")), new TermQuery(new Term(KEYWORD_FIELD_NAME, "hello")))
        );
    }

    /**
     * Query terms that contain "now" can trigger a query to not be cacheable.
     * This test checks the search context cacheable flag is updated accordingly.
     */
    public void testCachingStrategiesWithNow() throws IOException {
        // if we hit all fields, this should contain a date field and should diable cachability
        String query = "now " + randomAlphaOfLengthBetween(4, 10);
        QueryStringQueryBuilder queryStringQueryBuilder = new QueryStringQueryBuilder(query);
        assertQueryCachability(queryStringQueryBuilder, false);

        // if we hit a date field with "now", this should diable cachability
        queryStringQueryBuilder = new QueryStringQueryBuilder("now");
        queryStringQueryBuilder.field(DATE_FIELD_NAME);
        assertQueryCachability(queryStringQueryBuilder, false);

        // everything else is fine on all fields
        query = randomFrom("NoW", "nOw", "NOW") + " " + randomAlphaOfLengthBetween(4, 10);
        queryStringQueryBuilder = new QueryStringQueryBuilder(query);
        assertQueryCachability(queryStringQueryBuilder, true);
    }

    private void assertQueryCachability(QueryStringQueryBuilder qb, boolean cachingExpected) throws IOException {
        QueryShardContext context = createShardContext();
        assert context.isCacheable();
        /*
         * We use a private rewrite context here since we want the most realistic way of asserting that we are cacheable or not. We do it
         * this way in SearchService where we first rewrite the query with a private context, then reset the context and then build the
         * actual lucene query
         */
        QueryBuilder rewritten = rewriteQuery(qb, new QueryShardContext(context));
        assertNotNull(rewritten.toQuery(context));
        assertEquals(
            "query should " + (cachingExpected ? "" : "not") + " be cacheable: " + qb.toString(),
            cachingExpected,
            context.isCacheable()
        );
    }
}
