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

package org.opensearch.validate;

import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.admin.indices.validate.query.ValidateQueryResponse;
import org.opensearch.client.Client;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.MoreLikeThisQueryBuilder.Item;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.indices.TermsLookup;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchIntegTestCase.ClusterScope;
import org.opensearch.test.OpenSearchIntegTestCase.Scope;

import org.hamcrest.Matcher;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.index.query.QueryBuilders.queryStringQuery;
import static org.opensearch.index.query.QueryBuilders.rangeQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.allOf;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

@ClusterScope(scope = Scope.SUITE)
public class SimpleValidateQueryIT extends OpenSearchIntegTestCase {
    public void testSimpleValidateQuery() throws Exception {
        createIndex("test");
        ensureGreen();
        client().admin()
            .indices()
            .preparePutMapping("test")
            .setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject(MapperService.SINGLE_MAPPING_NAME)
                    .startObject("properties")
                    .startObject("foo")
                    .field("type", "text")
                    .endObject()
                    .startObject("bar")
                    .field("type", "integer")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .execute()
            .actionGet();

        refresh();

        assertThat(
            client().admin()
                .indices()
                .prepareValidateQuery("test")
                .setQuery(QueryBuilders.wrapperQuery("foo".getBytes(StandardCharsets.UTF_8)))
                .execute()
                .actionGet()
                .isValid(),
            equalTo(false)
        );
        assertThat(
            client().admin()
                .indices()
                .prepareValidateQuery("test")
                .setQuery(QueryBuilders.queryStringQuery("_id:1"))
                .execute()
                .actionGet()
                .isValid(),
            equalTo(true)
        );
        assertThat(
            client().admin()
                .indices()
                .prepareValidateQuery("test")
                .setQuery(QueryBuilders.queryStringQuery("_i:d:1"))
                .execute()
                .actionGet()
                .isValid(),
            equalTo(false)
        );

        assertThat(
            client().admin()
                .indices()
                .prepareValidateQuery("test")
                .setQuery(QueryBuilders.queryStringQuery("foo:1"))
                .execute()
                .actionGet()
                .isValid(),
            equalTo(true)
        );
        assertThat(
            client().admin()
                .indices()
                .prepareValidateQuery("test")
                .setQuery(QueryBuilders.queryStringQuery("bar:hey").lenient(false))
                .execute()
                .actionGet()
                .isValid(),
            equalTo(false)
        );

        assertThat(
            client().admin()
                .indices()
                .prepareValidateQuery("test")
                .setQuery(QueryBuilders.queryStringQuery("nonexistent:hello"))
                .execute()
                .actionGet()
                .isValid(),
            equalTo(true)
        );

        assertThat(
            client().admin()
                .indices()
                .prepareValidateQuery("test")
                .setQuery(QueryBuilders.queryStringQuery("foo:1 AND"))
                .execute()
                .actionGet()
                .isValid(),
            equalTo(false)
        );
    }

    public void testExplainValidateQueryTwoNodes() throws IOException {
        createIndex("test");
        ensureGreen();
        client().admin()
            .indices()
            .preparePutMapping("test")
            .setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject(MapperService.SINGLE_MAPPING_NAME)
                    .startObject("properties")
                    .startObject("foo")
                    .field("type", "text")
                    .endObject()
                    .startObject("bar")
                    .field("type", "integer")
                    .endObject()
                    .startObject("baz")
                    .field("type", "text")
                    .field("analyzer", "standard")
                    .endObject()
                    .startObject("pin")
                    .startObject("properties")
                    .startObject("location")
                    .field("type", "geo_point")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .execute()
            .actionGet();

        refresh();

        for (Client client : internalCluster().getClients()) {
            ValidateQueryResponse response = client.admin()
                .indices()
                .prepareValidateQuery("test")
                .setQuery(QueryBuilders.wrapperQuery("foo".getBytes(StandardCharsets.UTF_8)))
                .setExplain(true)
                .execute()
                .actionGet();
            assertThat(response.isValid(), equalTo(false));
            assertThat(response.getQueryExplanation().size(), equalTo(1));
            assertThat(response.getQueryExplanation().get(0).getError(), containsString("Failed to derive xcontent"));
            assertThat(response.getQueryExplanation().get(0).getExplanation(), nullValue());

        }

        for (Client client : internalCluster().getClients()) {
            ValidateQueryResponse response = client.admin()
                .indices()
                .prepareValidateQuery("test")
                .setQuery(QueryBuilders.queryStringQuery("foo"))
                .setExplain(true)
                .execute()
                .actionGet();
            assertThat(response.isValid(), equalTo(true));
            assertThat(response.getQueryExplanation().size(), equalTo(1));
            assertThat(
                response.getQueryExplanation().get(0).getExplanation(),
                containsString("MatchNoDocsQuery(\"failed [bar] query, caused by number_format_exception:[For input string: \"foo\"]\")")
            );
            assertThat(response.getQueryExplanation().get(0).getExplanation(), containsString("foo:foo"));
            assertThat(response.getQueryExplanation().get(0).getExplanation(), containsString("baz:foo"));
            assertThat(response.getQueryExplanation().get(0).getError(), nullValue());
        }
    }

    // Issue #3629
    public void testExplainDateRangeInQueryString() {
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put(indexSettings()).put("index.number_of_shards", 1)));

        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        String aMonthAgo = DateTimeFormatter.ISO_LOCAL_DATE.format(now.plus(1, ChronoUnit.MONTHS));
        String aMonthFromNow = DateTimeFormatter.ISO_LOCAL_DATE.format(now.minus(1, ChronoUnit.MONTHS));

        client().prepareIndex("test").setId("1").setSource("past", aMonthAgo, "future", aMonthFromNow).get();

        refresh();

        ValidateQueryResponse response = client().admin()
            .indices()
            .prepareValidateQuery()
            .setQuery(queryStringQuery("past:[now-2M/d TO now/d]"))
            .setRewrite(true)
            .get();

        assertNoFailures(response);
        assertThat(response.getQueryExplanation().size(), equalTo(1));
        assertThat(response.getQueryExplanation().get(0).getError(), nullValue());

        long twoMonthsAgo = now.minus(2, ChronoUnit.MONTHS).truncatedTo(ChronoUnit.DAYS).toEpochSecond() * 1000;
        long rangeEnd = (now.plus(1, ChronoUnit.DAYS).truncatedTo(ChronoUnit.DAYS).toEpochSecond() * 1000) - 1;
        assertThat(response.getQueryExplanation().get(0).getExplanation(), equalTo("past:[" + twoMonthsAgo + " TO " + rangeEnd + "]"));
        assertThat(response.isValid(), equalTo(true));
    }

    public void testValidateEmptyCluster() {
        try {
            client().admin().indices().prepareValidateQuery().get();
            fail("Expected IndexNotFoundException");
        } catch (IndexNotFoundException e) {
            assertThat(e.getMessage(), is("no such index [null] and no indices exist"));
        }
    }

    public void testExplainNoQuery() {
        createIndex("test");
        ensureGreen();

        ValidateQueryResponse validateQueryResponse = client().admin().indices().prepareValidateQuery().setExplain(true).get();
        assertThat(validateQueryResponse.isValid(), equalTo(true));
        assertThat(validateQueryResponse.getQueryExplanation().size(), equalTo(1));
        assertThat(validateQueryResponse.getQueryExplanation().get(0).getIndex(), equalTo("test"));
        assertThat(validateQueryResponse.getQueryExplanation().get(0).getExplanation(), equalTo("*:*"));
    }

    public void testExplainFilteredAlias() {
        assertAcked(
            prepareCreate("test").setMapping("field", "type=text")
                .addAlias(new Alias("alias").filter(QueryBuilders.termQuery("field", "value1")))
        );
        ensureGreen();

        ValidateQueryResponse validateQueryResponse = client().admin()
            .indices()
            .prepareValidateQuery("alias")
            .setQuery(QueryBuilders.matchAllQuery())
            .setExplain(true)
            .get();
        assertThat(validateQueryResponse.isValid(), equalTo(true));
        assertThat(validateQueryResponse.getQueryExplanation().size(), equalTo(1));
        assertThat(validateQueryResponse.getQueryExplanation().get(0).getIndex(), equalTo("test"));
        assertThat(validateQueryResponse.getQueryExplanation().get(0).getExplanation(), containsString("field:value1"));
    }

    public void testExplainWithRewriteValidateQuery() throws Exception {
        client().admin()
            .indices()
            .prepareCreate("test")
            .setMapping("field", "type=text,analyzer=whitespace")
            .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 1))
            .get();
        client().prepareIndex("test").setId("1").setSource("field", "quick lazy huge brown pidgin").get();
        client().prepareIndex("test").setId("2").setSource("field", "the quick brown fox").get();
        client().prepareIndex("test").setId("3").setSource("field", "the quick lazy huge brown fox jumps over the tree").get();
        client().prepareIndex("test").setId("4").setSource("field", "the lazy dog quacks like a duck").get();
        refresh();

        // prefix queries
        assertExplanation(QueryBuilders.matchPhrasePrefixQuery("field", "qu"), containsString("field:quick"), true);
        assertExplanation(QueryBuilders.matchPhrasePrefixQuery("field", "ju"), containsString("field:jumps"), true);

        // common terms queries
        assertExplanation(
            QueryBuilders.commonTermsQuery("field", "huge brown pidgin").cutoffFrequency(1),
            containsString("+field:pidgin field:huge field:brown"),
            true
        );
        assertExplanation(QueryBuilders.commonTermsQuery("field", "the brown").analyzer("stop"), containsString("field:brown"), true);

        // match queries with cutoff frequency
        assertExplanation(
            QueryBuilders.matchQuery("field", "huge brown pidgin").cutoffFrequency(1),
            containsString("+field:pidgin field:huge field:brown"),
            true
        );
        assertExplanation(QueryBuilders.matchQuery("field", "the brown").analyzer("stop"), containsString("field:brown"), true);

        // fuzzy queries
        assertExplanation(
            QueryBuilders.fuzzyQuery("field", "the").fuzziness(Fuzziness.fromEdits(2)),
            containsString("field:the (field:tree)^0.3333333"),
            true
        );
        assertExplanation(QueryBuilders.fuzzyQuery("field", "jump"), containsString("(field:jumps)^0.75"), true);

        // more like this queries
        Item[] items = new Item[] { new Item(null, "1") };
        assertExplanation(
            QueryBuilders.moreLikeThisQuery(new String[] { "field" }, null, items)
                .include(true)
                .minTermFreq(1)
                .minDocFreq(1)
                .maxQueryTerms(2),
            containsString("field:huge field:pidgin"),
            true
        );
        assertExplanation(
            QueryBuilders.moreLikeThisQuery(new String[] { "field" }, new String[] { "the huge pidgin" }, null)
                .minTermFreq(1)
                .minDocFreq(1)
                .maxQueryTerms(2),
            containsString("field:huge field:pidgin"),
            true
        );
    }

    public void testExplainWithRewriteValidateQueryAllShards() throws Exception {
        client().admin()
            .indices()
            .prepareCreate("test")
            .setMapping("field", "type=text,analyzer=whitespace")
            .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 2).put("index.number_of_routing_shards", 2))
            .get();
        // We are relying on specific routing behaviors for the result to be right, so
        // we cannot randomize the number of shards or change ids here.
        client().prepareIndex("test").setId("1").setSource("field", "quick lazy huge brown pidgin").get();
        client().prepareIndex("test").setId("2").setSource("field", "the quick brown fox").get();
        client().prepareIndex("test").setId("3").setSource("field", "the quick lazy huge brown fox jumps over the tree").get();
        client().prepareIndex("test").setId("4").setSource("field", "the lazy dog quacks like a duck").get();
        refresh();

        // prefix queries
        assertExplanations(
            QueryBuilders.matchPhrasePrefixQuery("field", "qu"),
            Arrays.asList(equalTo("field:quick"), allOf(containsString("field:quick"), containsString("field:quacks"))),
            true,
            true
        );
        assertExplanations(
            QueryBuilders.matchPhrasePrefixQuery("field", "ju"),
            Arrays.asList(equalTo("field:jumps"), equalTo("field:\"ju*\"")),
            true,
            true
        );
    }

    public void testIrrelevantPropertiesBeforeQuery() throws IOException {
        createIndex("test");
        ensureGreen();
        refresh();

        assertThat(
            client().admin()
                .indices()
                .prepareValidateQuery("test")
                .setQuery(
                    QueryBuilders.wrapperQuery(new BytesArray("{\"foo\": \"bar\", \"query\": {\"term\" : { \"user\" : \"foobar\" }}}"))
                )
                .get()
                .isValid(),
            equalTo(false)
        );
    }

    public void testIrrelevantPropertiesAfterQuery() throws IOException {
        createIndex("test");
        ensureGreen();
        refresh();

        assertThat(
            client().admin()
                .indices()
                .prepareValidateQuery("test")
                .setQuery(
                    QueryBuilders.wrapperQuery(new BytesArray("{\"query\": {\"term\" : { \"user\" : \"foobar\" }}, \"foo\": \"bar\"}"))
                )
                .get()
                .isValid(),
            equalTo(false)
        );
    }

    private static void assertExplanation(QueryBuilder queryBuilder, Matcher<String> matcher, boolean withRewrite) {
        ValidateQueryResponse response = client().admin()
            .indices()
            .prepareValidateQuery("test")
            .setQuery(queryBuilder)
            .setExplain(true)
            .setRewrite(withRewrite)
            .execute()
            .actionGet();
        assertThat(response.getQueryExplanation().size(), equalTo(1));
        assertThat(response.getQueryExplanation().get(0).getError(), nullValue());
        assertThat(response.getQueryExplanation().get(0).getExplanation(), matcher);
        assertThat(response.isValid(), equalTo(true));
    }

    private static void assertExplanations(
        QueryBuilder queryBuilder,
        List<Matcher<String>> matchers,
        boolean withRewrite,
        boolean allShards
    ) {
        ValidateQueryResponse response = client().admin()
            .indices()
            .prepareValidateQuery("test")
            .setQuery(queryBuilder)
            .setExplain(true)
            .setRewrite(withRewrite)
            .setAllShards(allShards)
            .execute()
            .actionGet();
        assertThat(response.getQueryExplanation().size(), equalTo(matchers.size()));
        for (int i = 0; i < matchers.size(); i++) {
            assertThat(response.getQueryExplanation().get(i).getError(), nullValue());
            assertThat(response.getQueryExplanation().get(i).getExplanation(), matchers.get(i));
            assertThat(response.isValid(), equalTo(true));
        }
    }

    public void testExplainTermsQueryWithLookup() throws Exception {
        client().admin()
            .indices()
            .prepareCreate("twitter")
            .setMapping("user", "type=integer", "followers", "type=integer")
            .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 2).put("index.number_of_routing_shards", 2))
            .get();
        client().prepareIndex("twitter").setId("1").setSource("followers", new int[] { 1, 2, 3 }).get();
        refresh();

        TermsQueryBuilder termsLookupQuery = QueryBuilders.termsLookupQuery("user", new TermsLookup("twitter", "1", "followers"));
        ValidateQueryResponse response = client().admin()
            .indices()
            .prepareValidateQuery("twitter")
            .setQuery(termsLookupQuery)
            .setExplain(true)
            .execute()
            .actionGet();
        assertThat(response.isValid(), is(true));
    }

    // Issue: https://github.com/opensearch-project/OpenSearch/issues/2036
    public void testValidateDateRangeInQueryString() throws IOException {
        assertAcked(prepareCreate("test").setSettings(Settings.builder().put(indexSettings()).put("index.number_of_shards", 1)));

        assertAcked(
            client().admin()
                .indices()
                .preparePutMapping("test")
                .setSource(
                    XContentFactory.jsonBuilder()
                        .startObject()
                        .startObject(MapperService.SINGLE_MAPPING_NAME)
                        .startObject("properties")
                        .startObject("name")
                        .field("type", "keyword")
                        .endObject()
                        .startObject("timestamp")
                        .field("type", "date")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
        );

        client().prepareIndex("test").setId("1").setSource("name", "username", "timestamp", 200).get();
        refresh();

        ValidateQueryResponse response = client().admin()
            .indices()
            .prepareValidateQuery()
            .setQuery(
                QueryBuilders.boolQuery()
                    .must(rangeQuery("timestamp").gte(0).lte(100))
                    .must(queryStringQuery("username").allowLeadingWildcard(false))
            )
            .setRewrite(true)
            .get();

        assertNoFailures(response);
        assertThat(response.isValid(), is(true));

        // Use wildcard and date outside the range
        response = client().admin()
            .indices()
            .prepareValidateQuery()
            .setQuery(
                QueryBuilders.boolQuery()
                    .must(rangeQuery("timestamp").gte(0).lte(100))
                    .must(queryStringQuery("*erna*").allowLeadingWildcard(false))
            )
            .setRewrite(true)
            .get();

        assertNoFailures(response);
        assertThat(response.isValid(), is(false));

        // Use wildcard and date inside the range
        response = client().admin()
            .indices()
            .prepareValidateQuery()
            .setQuery(
                QueryBuilders.boolQuery()
                    .must(rangeQuery("timestamp").gte(0).lte(1000))
                    .must(queryStringQuery("*erna*").allowLeadingWildcard(false))
            )
            .setRewrite(true)
            .get();

        assertNoFailures(response);
        assertThat(response.isValid(), is(false));

        // Use wildcard and date inside the range (allow leading wildcard)
        response = client().admin()
            .indices()
            .prepareValidateQuery()
            .setQuery(QueryBuilders.boolQuery().must(rangeQuery("timestamp").gte(0).lte(1000)).must(queryStringQuery("*erna*")))
            .setRewrite(true)
            .get();

        assertNoFailures(response);
        assertThat(response.isValid(), is(true));

        // Use invalid date range
        response = client().admin()
            .indices()
            .prepareValidateQuery()
            .setQuery(QueryBuilders.boolQuery().must(rangeQuery("timestamp").gte("aaa").lte(100)))
            .setRewrite(true)
            .get();

        assertNoFailures(response);
        assertThat(response.isValid(), is(false));

    }
}
