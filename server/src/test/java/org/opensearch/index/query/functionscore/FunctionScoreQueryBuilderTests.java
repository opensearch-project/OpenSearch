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

package org.opensearch.index.query.functionscore;

import com.fasterxml.jackson.core.JsonParseException;

import org.apache.lucene.index.Term;
import org.apache.lucene.search.MatchAllDocsQuery;
import org.apache.lucene.search.MatchNoDocsQuery;
import org.apache.lucene.search.Query;
import org.apache.lucene.search.TermQuery;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.lucene.search.function.CombineFunction;
import org.opensearch.common.lucene.search.function.FieldValueFactorFunction;
import org.opensearch.common.lucene.search.function.FunctionScoreQuery;
import org.opensearch.common.lucene.search.function.WeightFactorFunction;
import org.opensearch.common.unit.DistanceUnit;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.XContentHelper;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.RandomQueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.WrapperQueryBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder.FilterFunctionBuilder;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.script.MockScriptEngine;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.MultiValueMode;
import org.opensearch.test.AbstractQueryTestCase;
import org.opensearch.test.TestGeoShapeFieldMapperPlugin;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.hamcrest.CoreMatchers;
import org.hamcrest.Matcher;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singletonList;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.query.QueryBuilders.functionScoreQuery;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.opensearch.index.query.functionscore.ScoreFunctionBuilders.fieldValueFactorFunction;
import static org.opensearch.index.query.functionscore.ScoreFunctionBuilders.randomFunction;
import static org.opensearch.index.query.functionscore.ScoreFunctionBuilders.weightFactorFunction;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.nullValue;

public class FunctionScoreQueryBuilderTests extends AbstractQueryTestCase<FunctionScoreQueryBuilder> {

    private static final String[] SHUFFLE_PROTECTED_FIELDS = new String[] {
        Script.PARAMS_PARSE_FIELD.getPreferredName(),
        ExponentialDecayFunctionBuilder.NAME,
        LinearDecayFunctionBuilder.NAME,
        GaussDecayFunctionBuilder.NAME };

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Arrays.asList(TestPlugin.class, TestGeoShapeFieldMapperPlugin.class);
    }

    @Override
    protected FunctionScoreQueryBuilder doCreateTestQueryBuilder() {
        FunctionScoreQueryBuilder functionScoreQueryBuilder = createRandomFunctionScoreBuilder();
        if (randomBoolean()) {
            functionScoreQueryBuilder.boostMode(randomFrom(CombineFunction.values()));
        }
        if (randomBoolean()) {
            functionScoreQueryBuilder.scoreMode(randomFrom(FunctionScoreQuery.ScoreMode.values()));
        }
        if (randomBoolean()) {
            functionScoreQueryBuilder.maxBoost(randomFloat());
        }
        if (randomBoolean()) {
            functionScoreQueryBuilder.setMinScore(randomFloat());
        }
        return functionScoreQueryBuilder;
    }

    @Override
    protected String[] shuffleProtectedFields() {
        // do not shuffle fields that may contain arbitrary content
        return SHUFFLE_PROTECTED_FIELDS;
    }

    @Override
    protected Set<String> getObjectsHoldingArbitraryContent() {
        // script_score.script.params can contain arbitrary parameters. no error is expected when adding additional objects
        // within the params object. Score functions get parsed in the data nodes, so they are not validated in the coord node.
        return new HashSet<>(
            Arrays.asList(
                Script.PARAMS_PARSE_FIELD.getPreferredName(),
                ExponentialDecayFunctionBuilder.NAME,
                LinearDecayFunctionBuilder.NAME,
                GaussDecayFunctionBuilder.NAME
            )
        );
    }

    /**
     * Creates a random function score query using only constructor params. The caller is responsible for randomizing fields set outside of
     * the constructor.
     */
    private static FunctionScoreQueryBuilder createRandomFunctionScoreBuilder() {
        switch (randomIntBetween(0, 3)) {
            case 0:
                FilterFunctionBuilder[] functions = new FilterFunctionBuilder[randomIntBetween(0, 3)];
                for (int i = 0; i < functions.length; i++) {
                    functions[i] = new FilterFunctionBuilder(RandomQueryBuilder.createQuery(random()), randomScoreFunction());
                }
                if (randomBoolean()) {
                    return new FunctionScoreQueryBuilder(RandomQueryBuilder.createQuery(random()), functions);
                }
                return new FunctionScoreQueryBuilder(functions);
            case 1:
                return new FunctionScoreQueryBuilder(randomScoreFunction());
            case 2:
                return new FunctionScoreQueryBuilder(RandomQueryBuilder.createQuery(random()), randomScoreFunction());
            case 3:
                return new FunctionScoreQueryBuilder(RandomQueryBuilder.createQuery(random()));
            default:
                throw new UnsupportedOperationException();
        }
    }

    private static ScoreFunctionBuilder<?> randomScoreFunction() {
        if (randomBoolean()) {
            return new WeightBuilder().setWeight(randomFloat());
        }
        ScoreFunctionBuilder<?> functionBuilder;
        switch (randomIntBetween(0, 3)) {
            case 0:
                DecayFunctionBuilder<?> decayFunctionBuilder = createRandomDecayFunction();
                if (randomBoolean()) {
                    decayFunctionBuilder.setMultiValueMode(randomFrom(MultiValueMode.values()));
                }
                functionBuilder = decayFunctionBuilder;
                break;
            case 1:
                FieldValueFactorFunctionBuilder fieldValueFactorFunctionBuilder = fieldValueFactorFunction(
                    fieldValueFactorCompatibleField()
                );
                if (randomBoolean()) {
                    fieldValueFactorFunctionBuilder.factor(randomFloat());
                }
                if (randomBoolean()) {
                    fieldValueFactorFunctionBuilder.missing(randomDouble());
                }
                if (randomBoolean()) {
                    fieldValueFactorFunctionBuilder.modifier(randomFrom(FieldValueFactorFunction.Modifier.values()));
                }
                functionBuilder = fieldValueFactorFunctionBuilder;
                break;
            case 2:
                String script = "1";
                Map<String, Object> params = Collections.emptyMap();
                functionBuilder = new ScriptScoreFunctionBuilder(new Script(ScriptType.INLINE, MockScriptEngine.NAME, script, params));
                break;
            case 3:
                RandomScoreFunctionBuilder randomScoreFunctionBuilder = new RandomScoreFunctionBuilderWithFixedSeed();
                if (randomBoolean()) { // sometimes provide no seed
                    if (randomBoolean()) {
                        randomScoreFunctionBuilder.seed(randomLong());
                    } else if (randomBoolean()) {
                        randomScoreFunctionBuilder.seed(randomInt());
                    } else {
                        randomScoreFunctionBuilder.seed(randomAlphaOfLengthBetween(1, 10));
                    }
                    randomScoreFunctionBuilder.setField(SeqNoFieldMapper.NAME); // guaranteed to exist
                }
                functionBuilder = randomScoreFunctionBuilder;
                break;
            default:
                throw new UnsupportedOperationException();
        }
        if (randomBoolean()) {
            functionBuilder.setWeight(randomFloat());
        }
        return functionBuilder;
    }

    /**
     * A random field compatible with FieldValueFactor.
     */
    private static String fieldValueFactorCompatibleField() {
        return randomFrom(INT_FIELD_NAME, DOUBLE_FIELD_NAME, DATE_FIELD_NAME);
    }

    /**
     * Create a random decay function setting all of its constructor parameters randomly. The caller is responsible for randomizing other
     * fields.
     */
    private static DecayFunctionBuilder<?> createRandomDecayFunction() {
        String field = randomFrom(INT_FIELD_NAME, DOUBLE_FIELD_NAME, DATE_FIELD_NAME, GEO_POINT_FIELD_NAME);
        Object origin;
        Object scale;
        Object offset;
        switch (field) {
            case GEO_POINT_FIELD_NAME:
                origin = new GeoPoint(randomDouble(), randomDouble()).geohash();
                scale = randomFrom(DistanceUnit.values()).toString(randomDouble());
                offset = randomFrom(DistanceUnit.values()).toString(randomDouble());
                break;
            case DATE_FIELD_NAME:
                origin = new DateTime(System.currentTimeMillis() - randomIntBetween(0, 1000000), DateTimeZone.UTC).toString();
                scale = randomTimeValue(1, 1000, "d", "h", "ms", "s", "m");
                offset = randomPositiveTimeValue();
                break;
            default:
                origin = randomBoolean() ? randomInt() : randomFloat();
                scale = randomBoolean() ? between(1, Integer.MAX_VALUE) : randomFloat() + Float.MIN_NORMAL;
                offset = randomBoolean() ? between(1, Integer.MAX_VALUE) : randomFloat() + Float.MIN_NORMAL;
                break;
        }
        offset = randomBoolean() ? null : offset;
        double decay = randomDouble();
        switch (randomIntBetween(0, 2)) {
            case 0:
                return new GaussDecayFunctionBuilder(field, origin, scale, offset, decay);
            case 1:
                return new ExponentialDecayFunctionBuilder(field, origin, scale, offset, decay);
            case 2:
                return new LinearDecayFunctionBuilder(field, origin, scale, offset, decay);
            default:
                throw new UnsupportedOperationException();
        }
    }

    @Override
    protected void doAssertLuceneQuery(FunctionScoreQueryBuilder queryBuilder, Query query, QueryShardContext context) throws IOException {
        Query wrappedQuery = queryBuilder.query().rewrite(context).toQuery(context);
        if (wrappedQuery instanceof MatchNoDocsQuery) {
            assertThat(query, CoreMatchers.instanceOf(MatchNoDocsQuery.class));
        } else {
            assertThat(query, CoreMatchers.instanceOf(FunctionScoreQuery.class));
        }
    }

    public void testIllegalArguments() {
        expectThrows(IllegalArgumentException.class, () -> new FunctionScoreQueryBuilder((QueryBuilder) null));
        expectThrows(IllegalArgumentException.class, () -> new FunctionScoreQueryBuilder((ScoreFunctionBuilder<?>) null));
        expectThrows(IllegalArgumentException.class, () -> new FunctionScoreQueryBuilder((FilterFunctionBuilder[]) null));
        expectThrows(IllegalArgumentException.class, () -> new FunctionScoreQueryBuilder(null, randomFunction()));
        expectThrows(IllegalArgumentException.class, () -> new FunctionScoreQueryBuilder(matchAllQuery(), (ScoreFunctionBuilder<?>) null));
        expectThrows(IllegalArgumentException.class, () -> new FunctionScoreQueryBuilder(matchAllQuery(), (FilterFunctionBuilder[]) null));
        expectThrows(IllegalArgumentException.class, () -> new FunctionScoreQueryBuilder(null, new FilterFunctionBuilder[0]));
        expectThrows(
            IllegalArgumentException.class,
            () -> new FunctionScoreQueryBuilder(matchAllQuery(), new FilterFunctionBuilder[] { null })
        );
        expectThrows(IllegalArgumentException.class, () -> new FilterFunctionBuilder((ScoreFunctionBuilder<?>) null));
        expectThrows(IllegalArgumentException.class, () -> new FilterFunctionBuilder(null, randomFunction()));
        expectThrows(IllegalArgumentException.class, () -> new FilterFunctionBuilder(matchAllQuery(), null));
        FunctionScoreQueryBuilder builder = new FunctionScoreQueryBuilder(matchAllQuery());
        expectThrows(IllegalArgumentException.class, () -> builder.scoreMode(null));
        expectThrows(IllegalArgumentException.class, () -> builder.boostMode(null));
        expectThrows(
            IllegalArgumentException.class,
            () -> new FunctionScoreQueryBuilder.FilterFunctionBuilder(new WeightBuilder().setWeight(-1 * randomFloat()))
        );
    }

    public void testParseFunctionsArray() throws IOException {
        String functionScoreQuery = "{\n"
            + "    \"function_score\":{\n"
            + "        \"query\":{\n"
            + "            \"term\":{\n"
            + "                \"field1\":\"value1\"\n"
            + "            }\n"
            + "        },\n"
            + "        \"functions\":  [\n"
            + "            {\n"
            + "                \"random_score\":  {\n"
            + "                    \"seed\":123456\n"
            + "                },\n"
            + "                \"weight\": 3,\n"
            + "                \"filter\": {\n"
            + "                    \"term\":{\n"
            + "                        \"field2\":\"value2\"\n"
            + "                    }\n"
            + "                }\n"
            + "            },\n"
            + "            {\n"
            + "                \"filter\": {\n"
            + "                    \"term\":{\n"
            + "                        \"field3\":\"value3\"\n"
            + "                    }\n"
            + "                },\n"
            + "                \"weight\": 9\n"
            + "            },\n"
            + "            {\n"
            + "                \"gauss\":  {\n"
            + "                    \"field_name\":  {\n"
            + "                        \"origin\":0.5,\n"
            + "                        \"scale\":0.6\n"
            + "                    }\n"
            + "                }\n"
            + "            }\n"
            + "        ],\n"
            + "        \"boost\" : 3,\n"
            + "        \"score_mode\" : \"avg\",\n"
            + "        \"boost_mode\" : \"replace\",\n"
            + "        \"max_boost\" : 10\n"
            + "    }\n"
            + "}";

        QueryBuilder queryBuilder = parseQuery(functionScoreQuery);
        /*
         * given that we copy part of the decay functions as bytes, we test that fromXContent and toXContent both work no matter what the
         * initial format was
         */
        for (MediaType xContentType : XContentType.values()) {
            assertThat(queryBuilder, instanceOf(FunctionScoreQueryBuilder.class));
            FunctionScoreQueryBuilder functionScoreQueryBuilder = (FunctionScoreQueryBuilder) queryBuilder;
            assertThat(functionScoreQueryBuilder.query(), instanceOf(TermQueryBuilder.class));
            TermQueryBuilder termQueryBuilder = (TermQueryBuilder) functionScoreQueryBuilder.query();
            assertThat(termQueryBuilder.fieldName(), equalTo("field1"));
            assertThat(termQueryBuilder.value(), equalTo("value1"));
            assertThat(functionScoreQueryBuilder.filterFunctionBuilders().length, equalTo(3));
            assertThat(functionScoreQueryBuilder.filterFunctionBuilders()[0].getFilter(), instanceOf(TermQueryBuilder.class));
            termQueryBuilder = (TermQueryBuilder) functionScoreQueryBuilder.filterFunctionBuilders()[0].getFilter();
            assertThat(termQueryBuilder.fieldName(), equalTo("field2"));
            assertThat(termQueryBuilder.value(), equalTo("value2"));
            assertThat(functionScoreQueryBuilder.filterFunctionBuilders()[1].getFilter(), instanceOf(TermQueryBuilder.class));
            termQueryBuilder = (TermQueryBuilder) functionScoreQueryBuilder.filterFunctionBuilders()[1].getFilter();
            assertThat(termQueryBuilder.fieldName(), equalTo("field3"));
            assertThat(termQueryBuilder.value(), equalTo("value3"));
            assertThat(functionScoreQueryBuilder.filterFunctionBuilders()[2].getFilter(), instanceOf(MatchAllQueryBuilder.class));
            assertThat(
                functionScoreQueryBuilder.filterFunctionBuilders()[0].getScoreFunction(),
                instanceOf(RandomScoreFunctionBuilder.class)
            );
            RandomScoreFunctionBuilder randomScoreFunctionBuilder = (RandomScoreFunctionBuilder) functionScoreQueryBuilder
                .filterFunctionBuilders()[0].getScoreFunction();
            assertThat(randomScoreFunctionBuilder.getSeed(), equalTo(123456));
            assertThat(randomScoreFunctionBuilder.getWeight(), equalTo(3f));
            assertThat(functionScoreQueryBuilder.filterFunctionBuilders()[1].getScoreFunction(), instanceOf(WeightBuilder.class));
            WeightBuilder weightBuilder = (WeightBuilder) functionScoreQueryBuilder.filterFunctionBuilders()[1].getScoreFunction();
            assertThat(weightBuilder.getWeight(), equalTo(9f));
            assertThat(
                functionScoreQueryBuilder.filterFunctionBuilders()[2].getScoreFunction(),
                instanceOf(GaussDecayFunctionBuilder.class)
            );
            GaussDecayFunctionBuilder gaussDecayFunctionBuilder = (GaussDecayFunctionBuilder) functionScoreQueryBuilder
                .filterFunctionBuilders()[2].getScoreFunction();
            assertThat(gaussDecayFunctionBuilder.getFieldName(), equalTo("field_name"));
            assertThat(functionScoreQueryBuilder.boost(), equalTo(3f));
            assertThat(functionScoreQueryBuilder.scoreMode(), equalTo(FunctionScoreQuery.ScoreMode.AVG));
            assertThat(functionScoreQueryBuilder.boostMode(), equalTo(CombineFunction.REPLACE));
            assertThat(functionScoreQueryBuilder.maxBoost(), equalTo(10f));
            BytesReference bytes = XContentHelper.toXContent(queryBuilder, xContentType, false);
            try (XContentParser parser = createParser(xContentType.xContent(), bytes)) {
                queryBuilder = parseQuery(parser);
            }
        }
    }

    public void testParseSingleFunction() throws IOException {
        String functionScoreQuery = "{\n"
            + "    \"function_score\":{\n"
            + "        \"query\":{\n"
            + "            \"term\":{\n"
            + "                \"field1\":\"value1\"\n"
            + "            }\n"
            + "        },\n"
            + "        \"gauss\":  {\n"
            + "            \"field_name\":  {\n"
            + "                \"origin\":0.5,\n"
            + "                \"scale\":0.6\n"
            + "            }\n"
            + "         },\n"
            + "        \"boost\" : 3,\n"
            + "        \"score_mode\" : \"avg\",\n"
            + "        \"boost_mode\" : \"replace\",\n"
            + "        \"max_boost\" : 10\n"
            + "    }\n"
            + "}";

        QueryBuilder queryBuilder = parseQuery(functionScoreQuery);
        /*
         * given that we copy part of the decay functions as bytes, we test that fromXContent and toXContent both work no matter what the
         * initial format was
         */
        for (XContentType xContentType : XContentType.values()) {
            assertThat(queryBuilder, instanceOf(FunctionScoreQueryBuilder.class));
            FunctionScoreQueryBuilder functionScoreQueryBuilder = (FunctionScoreQueryBuilder) queryBuilder;
            assertThat(functionScoreQueryBuilder.query(), instanceOf(TermQueryBuilder.class));
            TermQueryBuilder termQueryBuilder = (TermQueryBuilder) functionScoreQueryBuilder.query();
            assertThat(termQueryBuilder.fieldName(), equalTo("field1"));
            assertThat(termQueryBuilder.value(), equalTo("value1"));
            assertThat(functionScoreQueryBuilder.filterFunctionBuilders().length, equalTo(1));
            assertThat(functionScoreQueryBuilder.filterFunctionBuilders()[0].getFilter(), instanceOf(MatchAllQueryBuilder.class));
            assertThat(
                functionScoreQueryBuilder.filterFunctionBuilders()[0].getScoreFunction(),
                instanceOf(GaussDecayFunctionBuilder.class)
            );
            GaussDecayFunctionBuilder gaussDecayFunctionBuilder = (GaussDecayFunctionBuilder) functionScoreQueryBuilder
                .filterFunctionBuilders()[0].getScoreFunction();
            assertThat(gaussDecayFunctionBuilder.getFieldName(), equalTo("field_name"));
            assertThat(gaussDecayFunctionBuilder.getWeight(), nullValue());
            assertThat(functionScoreQueryBuilder.boost(), equalTo(3f));
            assertThat(functionScoreQueryBuilder.scoreMode(), equalTo(FunctionScoreQuery.ScoreMode.AVG));
            assertThat(functionScoreQueryBuilder.boostMode(), equalTo(CombineFunction.REPLACE));
            assertThat(functionScoreQueryBuilder.maxBoost(), equalTo(10f));
            BytesReference bytes = XContentHelper.toXContent(queryBuilder, xContentType, false);
            try (XContentParser parser = createParser(xContentType.xContent(), bytes)) {
                queryBuilder = parseQuery(parser);
            }
        }
    }

    public void testProperErrorMessageWhenTwoFunctionsDefinedInQueryBody() throws IOException {
        // without a functions array, we support only a single function, weight can't be associated with the function either.
        String functionScoreQuery = "{\n"
            + "    \"function_score\": {\n"
            + "      \"script_score\": {\n"
            + "        \"script\": \"5\"\n"
            + "      },\n"
            + "      \"weight\": 2\n"
            + "    }\n"
            + "}";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(functionScoreQuery));
        assertThat(e.getMessage(), containsString("use [functions] array if you want to define several functions."));
    }

    public void testProperErrorMessageWhenTwoFunctionsDefinedInFunctionsArray() throws IOException {
        String functionScoreQuery = "{\n"
            + "    \"function_score\":{\n"
            + "        \"functions\":  [\n"
            + "            {\n"
            + "                \"random_score\":  {\n"
            + "                    \"seed\":123456\n"
            + "                },\n"
            + "                \"weight\": 3,\n"
            + "                \"script_score\": {\n"
            + "                    \"script\": \"_index['text']['foo'].tf()\"\n"
            + "                },\n"
            + "                \"filter\": {\n"
            + "                    \"term\":{\n"
            + "                        \"field2\":\"value2\"\n"
            + "                    }\n"
            + "                }\n"
            + "            }\n"
            + "        ]\n"
            + "    }\n"
            + "}";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(functionScoreQuery));
        assertThat(
            e.getMessage(),
            containsString("failed to parse function_score functions. already found [random_score], now encountering [script_score].")
        );
    }

    public void testProperErrorMessageWhenMissingFunction() throws IOException {
        String functionScoreQuery = "{\n"
            + "    \"function_score\":{\n"
            + "        \"functions\":  [\n"
            + "            {\n"
            + "                \"filter\": {\n"
            + "                    \"term\":{\n"
            + "                        \"field2\":\"value2\"\n"
            + "                    }\n"
            + "                }\n"
            + "            }\n"
            + "        ]\n"
            + "    }\n"
            + "}";
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(functionScoreQuery));
        assertThat(e.getMessage(), containsString("an entry in functions list is missing a function."));
    }

    public void testWeight1fStillProducesWeightFunction() throws IOException {
        String queryString = jsonBuilder().startObject()
            .startObject("function_score")
            .startArray("functions")
            .startObject()
            .startObject("field_value_factor")
            .field("field", INT_FIELD_NAME)
            .endObject()
            .field("weight", 1.0)
            .endObject()
            .endArray()
            .endObject()
            .endObject()
            .toString();
        QueryBuilder query = parseQuery(queryString);
        assertThat(query, instanceOf(FunctionScoreQueryBuilder.class));
        FunctionScoreQueryBuilder functionScoreQueryBuilder = (FunctionScoreQueryBuilder) query;
        assertThat(
            functionScoreQueryBuilder.filterFunctionBuilders()[0].getScoreFunction(),
            instanceOf(FieldValueFactorFunctionBuilder.class)
        );
        FieldValueFactorFunctionBuilder fieldValueFactorFunctionBuilder = (FieldValueFactorFunctionBuilder) functionScoreQueryBuilder
            .filterFunctionBuilders()[0].getScoreFunction();
        assertThat(fieldValueFactorFunctionBuilder.fieldName(), equalTo(INT_FIELD_NAME));
        assertThat(fieldValueFactorFunctionBuilder.factor(), equalTo(FieldValueFactorFunctionBuilder.DEFAULT_FACTOR));
        assertThat(fieldValueFactorFunctionBuilder.modifier(), equalTo(FieldValueFactorFunctionBuilder.DEFAULT_MODIFIER));
        assertThat(fieldValueFactorFunctionBuilder.getWeight(), equalTo(1f));
        assertThat(fieldValueFactorFunctionBuilder.missing(), nullValue());

        Query luceneQuery = query.toQuery(createShardContext());
        assertThat(luceneQuery, instanceOf(FunctionScoreQuery.class));
        FunctionScoreQuery functionScoreQuery = (FunctionScoreQuery) luceneQuery;
        assertThat(functionScoreQuery.getFunctions().length, equalTo(1));
        assertThat(functionScoreQuery.getFunctions()[0], instanceOf(WeightFactorFunction.class));
        WeightFactorFunction weightFactorFunction = (WeightFactorFunction) functionScoreQuery.getFunctions()[0];
        assertThat(weightFactorFunction.getWeight(), equalTo(1.0f));
        assertThat(weightFactorFunction.getScoreFunction(), instanceOf(FieldValueFactorFunction.class));
    }

    public void testProperErrorMessagesForMisplacedWeightsAndFunctions() throws IOException {
        String query = jsonBuilder().startObject()
            .startObject("function_score")
            .startArray("functions")
            .startObject()
            .startObject("script_score")
            .field("script", "3")
            .endObject()
            .endObject()
            .endArray()
            .field("weight", 2)
            .endObject()
            .endObject()
            .toString();
        expectParsingException(
            query,
            "[you can either define [functions] array or a single function, not both. already "
                + "found [functions] array, now encountering [weight].]"
        );
        query = jsonBuilder().startObject()
            .startObject("function_score")
            .field("weight", 2)
            .startArray("functions")
            .startObject()
            .endObject()
            .endArray()
            .endObject()
            .endObject()
            .toString();
        expectParsingException(
            query,
            "[you can either define [functions] array or a single function, not both. already found "
                + "[weight], now encountering [functions].]"
        );
    }

    public void testMalformedThrowsException() throws IOException {
        String json = "{\n"
            + "    \"function_score\":{\n"
            + "        \"query\":{\n"
            + "            \"term\":{\n"
            + "                \"name.last\":\"banon\"\n"
            + "            }\n"
            + "        },\n"
            + "        \"functions\": [\n"
            + "            {\n"
            + "                {\n"
            + "            }\n"
            + "        ]\n"
            + "    }\n"
            + "}";
        JsonParseException e = expectThrows(JsonParseException.class, () -> parseQuery(json));
        assertThat(e.getMessage(), containsString("Unexpected character ('{"));
    }

    public void testCustomWeightFactorQueryBuilderWithFunctionScore() throws IOException {
        QueryShardContext context = createShardContext();
        Query parsedQuery = parseQuery(functionScoreQuery(termQuery(KEYWORD_FIELD_NAME, "banon"), weightFactorFunction(1.3f))).rewrite(
            context
        ).toQuery(context);
        assertThat(parsedQuery, instanceOf(FunctionScoreQuery.class));
        FunctionScoreQuery functionScoreQuery = (FunctionScoreQuery) parsedQuery;
        assertThat(((TermQuery) functionScoreQuery.getSubQuery()).getTerm(), equalTo(new Term(KEYWORD_FIELD_NAME, "banon")));
        assertThat((double) (functionScoreQuery.getFunctions()[0]).getWeight(), closeTo(1.3, 0.001));
    }

    public void testCustomWeightFactorQueryBuilderWithFunctionScoreWithoutQueryGiven() throws IOException {
        Query parsedQuery = parseQuery(functionScoreQuery(weightFactorFunction(1.3f))).toQuery(createShardContext());
        assertThat(parsedQuery, instanceOf(FunctionScoreQuery.class));
        FunctionScoreQuery functionScoreQuery = (FunctionScoreQuery) parsedQuery;
        assertThat(functionScoreQuery.getSubQuery() instanceof MatchAllDocsQuery, equalTo(true));
        assertThat((double) (functionScoreQuery.getFunctions()[0]).getWeight(), closeTo(1.3, 0.001));
    }

    public void testFieldValueFactorFactorArray() throws IOException {
        // don't permit an array of factors
        String querySource = "{"
            + "  \"function_score\": {"
            + "    \"query\": {"
            + "      \"match\": {\"name\": \"foo\"}"
            + "      },"
            + "      \"functions\": ["
            + "        {"
            + "          \"field_value_factor\": {"
            + "            \"field\": \"test\","
            + "            \"factor\": [1.2,2]"
            + "          }"
            + "        }"
            + "      ]"
            + "    }"
            + "}";
        expectParsingException(querySource, containsString("[field_value_factor] field 'factor' does not support lists or objects"));
    }

    public void testFromJson() throws IOException {
        String json = "{\n"
            + "  \"function_score\" : {\n"
            + "    \"query\" : { \"match_all\" : {} },\n"
            + "    \"functions\" : [ {\n"
            + "      \"filter\" : { \"match_all\" : {}},\n"
            + "      \"weight\" : 23.0,\n"
            + "      \"random_score\" : { }\n"
            + "    }, {\n"
            + "      \"filter\" : { \"match_all\" : {}},\n"
            + "      \"weight\" : 5.0\n"
            + "    } ],\n"
            + "    \"score_mode\" : \"multiply\",\n"
            + "    \"boost_mode\" : \"multiply\",\n"
            + "    \"max_boost\" : 100.0,\n"
            + "    \"min_score\" : 1.0,\n"
            + "    \"boost\" : 42.0\n"
            + "  }\n"
            + "}";

        FunctionScoreQueryBuilder parsed = (FunctionScoreQueryBuilder) parseQuery(json);
        // this should be equivalent to the same with a match_all query
        String expected = "{\n"
            + "  \"function_score\" : {\n"
            + "    \"query\" : { \"match_all\" : {} },\n"
            + "    \"functions\" : [ {\n"
            + "      \"filter\" : { \"match_all\" : {}},\n"
            + "      \"weight\" : 23.0,\n"
            + "      \"random_score\" : { }\n"
            + "    }, {\n"
            + "      \"filter\" : { \"match_all\" : {}},\n"
            + "      \"weight\" : 5.0\n"
            + "    } ],\n"
            + "    \"score_mode\" : \"multiply\",\n"
            + "    \"boost_mode\" : \"multiply\",\n"
            + "    \"max_boost\" : 100.0,\n"
            + "    \"min_score\" : 1.0,\n"
            + "    \"boost\" : 42.0\n"
            + "  }\n"
            + "}";

        FunctionScoreQueryBuilder expectedParsed = (FunctionScoreQueryBuilder) parseQuery(expected);
        assertEquals(expectedParsed, parsed);

        assertEquals(json, 2, parsed.filterFunctionBuilders().length);
        assertEquals(json, 42, parsed.boost(), 0.0001);
        assertEquals(json, 100, parsed.maxBoost(), 0.00001);
        assertEquals(json, 1, parsed.getMinScore(), 0.0001);
    }

    public void testRewrite() throws IOException {
        FunctionScoreQueryBuilder functionScoreQueryBuilder = new FunctionScoreQueryBuilder(
            new WrapperQueryBuilder(new TermQueryBuilder(TEXT_FIELD_NAME, "bar").toString())
        ).boostMode(CombineFunction.REPLACE).scoreMode(FunctionScoreQuery.ScoreMode.SUM).setMinScore(1).maxBoost(100);
        FunctionScoreQueryBuilder rewrite = (FunctionScoreQueryBuilder) functionScoreQueryBuilder.rewrite(createShardContext());
        assertNotSame(functionScoreQueryBuilder, rewrite);
        assertEquals(rewrite.query(), new TermQueryBuilder(TEXT_FIELD_NAME, "bar"));
        assertEquals(rewrite.boostMode(), CombineFunction.REPLACE);
        assertEquals(rewrite.scoreMode(), FunctionScoreQuery.ScoreMode.SUM);
        assertEquals(rewrite.getMinScore(), 1f, 0.0001);
        assertEquals(rewrite.maxBoost(), 100f, 0.0001);
    }

    public void testRewriteWithFunction() throws IOException {
        QueryBuilder firstFunction = new WrapperQueryBuilder(new TermQueryBuilder(KEYWORD_FIELD_NAME, "1").toString());
        TermQueryBuilder secondFunction = new TermQueryBuilder(KEYWORD_FIELD_NAME, "2");
        QueryBuilder queryBuilder = randomBoolean()
            ? new WrapperQueryBuilder(new TermQueryBuilder(TEXT_FIELD_NAME, "bar").toString())
            : new TermQueryBuilder(TEXT_FIELD_NAME, "bar");
        FunctionScoreQueryBuilder functionScoreQueryBuilder = new FunctionScoreQueryBuilder(
            queryBuilder,
            new FunctionScoreQueryBuilder.FilterFunctionBuilder[] {
                new FunctionScoreQueryBuilder.FilterFunctionBuilder(firstFunction, new RandomScoreFunctionBuilder()),
                new FunctionScoreQueryBuilder.FilterFunctionBuilder(secondFunction, new RandomScoreFunctionBuilder()) }
        );
        FunctionScoreQueryBuilder rewrite = (FunctionScoreQueryBuilder) functionScoreQueryBuilder.rewrite(createShardContext());
        assertNotSame(functionScoreQueryBuilder, rewrite);
        assertEquals(rewrite.query(), new TermQueryBuilder(TEXT_FIELD_NAME, "bar"));
        assertEquals(rewrite.filterFunctionBuilders()[0].getFilter(), new TermQueryBuilder(KEYWORD_FIELD_NAME, "1"));
        assertSame(rewrite.filterFunctionBuilders()[1].getFilter(), secondFunction);
    }

    public void testRewriteToMatchNone() throws IOException {
        FunctionScoreQueryBuilder functionScoreQueryBuilder = new FunctionScoreQueryBuilder(new TermQueryBuilder("unmapped_field", "value"))
            .boostMode(CombineFunction.REPLACE)
            .scoreMode(FunctionScoreQuery.ScoreMode.SUM)
            .setMinScore(1)
            .maxBoost(100);

        QueryBuilder rewrite = functionScoreQueryBuilder.rewrite(createShardContext());
        assertThat(rewrite, instanceOf(MatchNoneQueryBuilder.class));
    }

    /**
     * Please see https://github.com/elastic/elasticsearch/issues/35123 for context.
     */
    public void testSingleScriptFunction() throws IOException {
        QueryBuilder queryBuilder = termQuery(KEYWORD_FIELD_NAME, "value");
        ScoreFunctionBuilder<ScriptScoreFunctionBuilder> functionBuilder = new ScriptScoreFunctionBuilder(
            new Script(ScriptType.INLINE, MockScriptEngine.NAME, "1", Collections.emptyMap())
        );

        FunctionScoreQueryBuilder builder = functionScoreQuery(queryBuilder, functionBuilder);
        if (randomBoolean()) {
            builder.boostMode(randomFrom(CombineFunction.values()));
        }

        QueryShardContext shardContext = createShardContext();
        Query query = builder.rewrite(shardContext).toQuery(shardContext);
        assertThat(query, instanceOf(FunctionScoreQuery.class));

        CombineFunction expectedBoostMode = builder.boostMode() != null
            ? builder.boostMode()
            : FunctionScoreQueryBuilder.DEFAULT_BOOST_MODE;
        CombineFunction actualBoostMode = ((FunctionScoreQuery) query).getCombineFunction();
        assertEquals(expectedBoostMode, actualBoostMode);
    }

    public void testQueryMalformedArrayNotSupported() throws IOException {
        String json = "{\n" + "  \"function_score\" : {\n" + "    \"not_supported\" : []\n" + "  }\n" + "}";

        expectParsingException(json, "array [not_supported] is not supported");
    }

    public void testQueryMalformedFieldNotSupported() throws IOException {
        String json = "{\n" + "  \"function_score\" : {\n" + "    \"not_supported\" : \"value\"\n" + "  }\n" + "}";

        expectParsingException(json, "field [not_supported] is not supported");
    }

    public void testMalformedQueryFunctionFieldNotSupported() throws IOException {
        String json = "{\n"
            + "  \"function_score\" : {\n"
            + "    \"functions\" : [ {\n"
            + "      \"not_supported\" : 23.0\n"
            + "    }\n"
            + "  }\n"
            + "}";

        expectParsingException(json, "field [not_supported] is not supported");
    }

    public void testMalformedQueryMultipleQueryObjects() throws IOException {
        // verify that an error is thrown rather than setting the query twice (https://github.com/elastic/elasticsearch/issues/16583)
        String json = "{\n"
            + "    \"function_score\":{\n"
            + "        \"query\":{\n"
            + "            \"bool\":{\n"
            + "                \"must\":{\"match\":{\"field\":\"value\"}}"
            + "             },\n"
            + "            \"ignored_field_name\": {\n"
            + "                {\"match\":{\"field\":\"value\"}}\n"
            + "            }\n"
            + "            }\n"
            + "        }\n"
            + "    }\n"
            + "}";
        expectParsingException(json, equalTo("[bool] malformed query, expected [END_OBJECT] but found [FIELD_NAME]"));
    }

    private void expectParsingException(String json, Matcher<String> messageMatcher) {
        ParsingException e = expectThrows(ParsingException.class, () -> parseQuery(json));
        assertThat(e.getMessage(), messageMatcher);
    }

    private void expectParsingException(String json, String message) {
        expectParsingException(json, equalTo("failed to parse [function_score] query. " + message));
    }

    /**
     * A hack on top of the normal random score function that fixed toQuery to work properly in this unit testing environment.
     */
    static class RandomScoreFunctionBuilderWithFixedSeed extends RandomScoreFunctionBuilder {
        public static final String NAME = "random_with_fixed_seed";

        RandomScoreFunctionBuilderWithFixedSeed() {}

        /**
         * Read from a stream.
         */
        RandomScoreFunctionBuilderWithFixedSeed(StreamInput in) throws IOException {
            super(in);
        }

        @Override
        public String getName() {
            return NAME;
        }

        public static RandomScoreFunctionBuilder fromXContent(XContentParser parser) throws IOException, ParsingException {
            RandomScoreFunctionBuilder builder = RandomScoreFunctionBuilder.fromXContent(parser);
            RandomScoreFunctionBuilderWithFixedSeed replacement = new RandomScoreFunctionBuilderWithFixedSeed();
            if (builder.getSeed() != null) {
                replacement.seed(builder.getSeed());
            }
            return replacement;
        }
    }

    public static class TestPlugin extends Plugin implements SearchPlugin {
        @Override
        public List<ScoreFunctionSpec<?>> getScoreFunctions() {
            return singletonList(
                new ScoreFunctionSpec<>(
                    RandomScoreFunctionBuilderWithFixedSeed.NAME,
                    RandomScoreFunctionBuilderWithFixedSeed::new,
                    RandomScoreFunctionBuilderWithFixedSeed::fromXContent
                )
            );
        }
    }

    /**
     * Check that this query is generally cacheable except for builders using {@link ScriptScoreFunctionBuilder} or
     * {@link RandomScoreFunctionBuilder} without a seed
     */
    @Override
    public void testCacheability() throws IOException {
        FunctionScoreQueryBuilder queryBuilder = createTestQueryBuilder();
        boolean isCacheable = isCacheable(queryBuilder);
        QueryShardContext context = createShardContext();
        QueryBuilder rewriteQuery = rewriteQuery(queryBuilder, new QueryShardContext(context));
        assertNotNull(rewriteQuery.toQuery(context));
        // we occasionally need to update the expected "isCacheable" flag after rewrite for MatchNoneQueryBuilder
        if (rewriteQuery instanceof MatchNoneQueryBuilder) {
            isCacheable = true;
        }
        assertEquals(
            "query should " + (isCacheable ? "" : "not") + " be cacheable: " + queryBuilder.toString(),
            isCacheable,
            context.isCacheable()
        );

        ScoreFunctionBuilder<?> scriptScoreFunction = new ScriptScoreFunctionBuilder(
            new Script(ScriptType.INLINE, MockScriptEngine.NAME, "1", Collections.emptyMap())
        );
        queryBuilder = new FunctionScoreQueryBuilder(
            new FilterFunctionBuilder[] { new FilterFunctionBuilder(RandomQueryBuilder.createQuery(random()), scriptScoreFunction) }
        );
        context = createShardContext();
        rewriteQuery = rewriteQuery(queryBuilder, new QueryShardContext(context));
        assertNotNull(rewriteQuery.toQuery(context));
        assertTrue("function script query should be cacheable" + queryBuilder.toString(), context.isCacheable());

        RandomScoreFunctionBuilder randomScoreFunctionBuilder = new RandomScoreFunctionBuilderWithFixedSeed();
        queryBuilder = new FunctionScoreQueryBuilder(
            new FilterFunctionBuilder[] { new FilterFunctionBuilder(RandomQueryBuilder.createQuery(random()), randomScoreFunctionBuilder) }
        );
        context = createShardContext();
        rewriteQuery = rewriteQuery(queryBuilder, new QueryShardContext(context));
        assertNotNull(rewriteQuery.toQuery(context));
        assertFalse("function random query should not be cacheable: " + queryBuilder.toString(), context.isCacheable());
    }

    private boolean isCacheable(FunctionScoreQueryBuilder queryBuilder) {
        FilterFunctionBuilder[] filterFunctionBuilders = queryBuilder.filterFunctionBuilders();
        for (FilterFunctionBuilder builder : filterFunctionBuilders) {
            if (builder.getScoreFunction() instanceof RandomScoreFunctionBuilder
                && ((RandomScoreFunctionBuilder) builder.getScoreFunction()).getSeed() == null) {
                return false;
            }
        }
        return true;
    }

    @Override
    public void testMustRewrite() throws IOException {
        QueryShardContext context = createShardContext();
        context.setAllowUnmappedFields(true);
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("unmapped_field", "foo");

        // main query needs rewriting
        FunctionScoreQueryBuilder functionQueryBuilder1 = new FunctionScoreQueryBuilder(termQueryBuilder);
        functionQueryBuilder1.setMinScore(1);
        IllegalStateException e = expectThrows(IllegalStateException.class, () -> functionQueryBuilder1.toQuery(context));
        assertEquals("Rewrite first", e.getMessage());

        // filter needs rewriting
        FunctionScoreQueryBuilder functionQueryBuilder2 = new FunctionScoreQueryBuilder(
            new MatchAllQueryBuilder(),
            new FilterFunctionBuilder[] { new FilterFunctionBuilder(termQueryBuilder, new RandomScoreFunctionBuilder()) }
        );
        e = expectThrows(IllegalStateException.class, () -> functionQueryBuilder2.toQuery(context));
        assertEquals("Rewrite first", e.getMessage());
    }

    public void testVisit() {
        TermQueryBuilder termQueryBuilder = new TermQueryBuilder("unmapped_field", "foo");
        FunctionScoreQueryBuilder builder = new FunctionScoreQueryBuilder(termQueryBuilder);

        List<QueryBuilder> visitedQueries = new ArrayList<>();
        builder.visit(createTestVisitor(visitedQueries));

        assertEquals(2, visitedQueries.size());
    }
}
