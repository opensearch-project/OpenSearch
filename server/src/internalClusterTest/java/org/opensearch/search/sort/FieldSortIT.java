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

package org.opensearch.search.sort;

import org.apache.lucene.util.BytesRef;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.lucene.util.UnicodeUtil;

import org.opensearch.OpenSearchException;
import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Numbers;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.fielddata.ScriptDocValues;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.functionscore.ScoreFunctionBuilders;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.Plugin;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.script.MockScriptPlugin;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.InternalSettingsPlugin;

import org.hamcrest.Matchers;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.query.QueryBuilders.functionScoreQuery;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.index.query.functionscore.ScoreFunctionBuilders.fieldValueFactorFunction;
import static org.opensearch.script.MockScriptPlugin.NAME;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertFirstHit;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSecondHit;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.hasId;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.oneOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class FieldSortIT extends OpenSearchIntegTestCase {
    public static class CustomScriptPlugin extends MockScriptPlugin {
        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();
            scripts.put("doc['number'].value", vars -> sortDoubleScript(vars));
            scripts.put("doc['keyword'].value", vars -> sortStringScript(vars));
            return scripts;
        }

        static Double sortDoubleScript(Map<String, Object> vars) {
            Map<?, ?> doc = (Map) vars.get("doc");
            Double index = ((Number) ((ScriptDocValues<?>) doc.get("number")).get(0)).doubleValue();
            return index;
        }

        static String sortStringScript(Map<String, Object> vars) {
            Map<?, ?> doc = (Map) vars.get("doc");
            String value = ((String) ((ScriptDocValues<?>) doc.get("keyword")).get(0));
            return value;
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(InternalSettingsPlugin.class, CustomScriptPlugin.class);
    }

    public void testIssue8226() {
        int numIndices = between(5, 10);
        final boolean useMapping = randomBoolean();
        for (int i = 0; i < numIndices; i++) {
            if (useMapping) {
                assertAcked(prepareCreate("test_" + i).addAlias(new Alias("test")).setMapping("entry", "type=long"));
            } else {
                assertAcked(prepareCreate("test_" + i).addAlias(new Alias("test")));
            }
            if (i > 0) {
                client().prepareIndex("test_" + i).setId("" + i).setSource("{\"entry\": " + i + "}", XContentType.JSON).get();
            }
        }
        refresh();
        // sort DESC
        SearchResponse searchResponse = client().prepareSearch()
            .addSort(new FieldSortBuilder("entry").order(SortOrder.DESC).unmappedType(useMapping ? null : "long"))
            .setSize(10)
            .get();
        logClusterState();
        assertSearchResponse(searchResponse);

        for (int j = 1; j < searchResponse.getHits().getHits().length; j++) {
            Number current = (Number) searchResponse.getHits().getHits()[j].getSourceAsMap().get("entry");
            Number previous = (Number) searchResponse.getHits().getHits()[j - 1].getSourceAsMap().get("entry");
            assertThat(searchResponse.toString(), current.intValue(), lessThan(previous.intValue()));
        }

        // sort ASC
        searchResponse = client().prepareSearch()
            .addSort(new FieldSortBuilder("entry").order(SortOrder.ASC).unmappedType(useMapping ? null : "long"))
            .setSize(10)
            .get();
        logClusterState();
        assertSearchResponse(searchResponse);

        for (int j = 1; j < searchResponse.getHits().getHits().length; j++) {
            Number current = (Number) searchResponse.getHits().getHits()[j].getSourceAsMap().get("entry");
            Number previous = (Number) searchResponse.getHits().getHits()[j - 1].getSourceAsMap().get("entry");
            assertThat(searchResponse.toString(), current.intValue(), greaterThan(previous.intValue()));
        }
    }

    public void testIssue6614() throws ExecutionException, InterruptedException {
        List<IndexRequestBuilder> builders = new ArrayList<>();
        boolean strictTimeBasedIndices = randomBoolean();
        final int numIndices = randomIntBetween(2, 25); // at most 25 days in the month
        int docs = 0;
        for (int i = 0; i < numIndices; i++) {
            final String indexId = strictTimeBasedIndices ? "idx_" + i : "idx";
            if (strictTimeBasedIndices || i == 0) {
                createIndex(indexId);
            }
            final int numDocs = randomIntBetween(1, 23);  // hour of the day
            for (int j = 0; j < numDocs; j++) {
                builders.add(
                    client().prepareIndex(indexId)
                        .setSource(
                            "foo",
                            "bar",
                            "timeUpdated",
                            "2014/07/"
                                + String.format(Locale.ROOT, "%02d", i + 1)
                                + " "
                                + String.format(Locale.ROOT, "%02d", j + 1)
                                + ":00:00"
                        )
                );
            }
            indexRandom(true, builders);
            docs += builders.size();
            builders.clear();
        }
        SearchResponse allDocsResponse = client().prepareSearch()
            .setQuery(
                QueryBuilders.boolQuery()
                    .must(QueryBuilders.termQuery("foo", "bar"))
                    .must(QueryBuilders.rangeQuery("timeUpdated").gte("2014/0" + randomIntBetween(1, 7) + "/01"))
            )
            .addSort(new FieldSortBuilder("timeUpdated").order(SortOrder.ASC).unmappedType("date"))
            .setSize(docs)
            .get();
        assertSearchResponse(allDocsResponse);

        final int numiters = randomIntBetween(1, 20);
        for (int i = 0; i < numiters; i++) {
            SearchResponse searchResponse = client().prepareSearch()
                .setQuery(
                    QueryBuilders.boolQuery()
                        .must(QueryBuilders.termQuery("foo", "bar"))
                        .must(
                            QueryBuilders.rangeQuery("timeUpdated")
                                .gte("2014/" + String.format(Locale.ROOT, "%02d", randomIntBetween(1, 7)) + "/01")
                        )
                )
                .addSort(new FieldSortBuilder("timeUpdated").order(SortOrder.ASC).unmappedType("date"))
                .setSize(scaledRandomIntBetween(1, docs))
                .get();
            assertSearchResponse(searchResponse);
            for (int j = 0; j < searchResponse.getHits().getHits().length; j++) {
                assertThat(
                    searchResponse.toString() + "\n vs. \n" + allDocsResponse.toString(),
                    searchResponse.getHits().getHits()[j].getId(),
                    equalTo(allDocsResponse.getHits().getHits()[j].getId())
                );
            }
        }

    }

    public void testTrackScores() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").setMapping("svalue", "type=keyword").get());
        ensureGreen();
        index(
            "test",
            "type1",
            jsonBuilder().startObject().field("id", "1").field("svalue", "aaa").field("ivalue", 100).field("dvalue", 0.1).endObject()
        );
        index(
            "test",
            "type1",
            jsonBuilder().startObject().field("id", "2").field("svalue", "bbb").field("ivalue", 200).field("dvalue", 0.2).endObject()
        );
        refresh();

        SearchResponse searchResponse = client().prepareSearch().setQuery(matchAllQuery()).addSort("svalue", SortOrder.ASC).get();

        assertThat(searchResponse.getHits().getMaxScore(), equalTo(Float.NaN));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.getScore(), equalTo(Float.NaN));
        }

        // now check with score tracking
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).addSort("svalue", SortOrder.ASC).setTrackScores(true).get();

        assertThat(searchResponse.getHits().getMaxScore(), not(equalTo(Float.NaN)));
        for (SearchHit hit : searchResponse.getHits()) {
            assertThat(hit.getScore(), not(equalTo(Float.NaN)));
        }
    }

    public void testRandomSorting() throws IOException, InterruptedException, ExecutionException {
        Random random = random();
        assertAcked(
            prepareCreate("test").setMapping(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject("sparse_bytes")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("dense_bytes")
                    .field("type", "keyword")
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        ensureGreen();

        TreeMap<BytesRef, String> sparseBytes = new TreeMap<>();
        TreeMap<BytesRef, String> denseBytes = new TreeMap<>();
        int numDocs = randomIntBetween(200, 300);
        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; i++) {
            String docId = Integer.toString(i);
            BytesRef ref = null;
            do {
                ref = new BytesRef(TestUtil.randomRealisticUnicodeString(random));
            } while (denseBytes.containsKey(ref));
            denseBytes.put(ref, docId);
            XContentBuilder src = jsonBuilder().startObject().field("dense_bytes", ref.utf8ToString());
            if (rarely()) {
                src.field("sparse_bytes", ref.utf8ToString());
                sparseBytes.put(ref, docId);
            }
            src.endObject();
            builders[i] = client().prepareIndex("test").setId(docId).setSource(src);
        }
        indexRandom(true, builders);
        {
            int size = between(1, denseBytes.size());
            SearchResponse searchResponse = client().prepareSearch("test")
                .setQuery(matchAllQuery())
                .setSize(size)
                .addSort("dense_bytes", SortOrder.ASC)
                .get();
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo((long) numDocs));
            assertThat(searchResponse.getHits().getHits().length, equalTo(size));
            Set<Entry<BytesRef, String>> entrySet = denseBytes.entrySet();
            Iterator<Entry<BytesRef, String>> iterator = entrySet.iterator();
            for (int i = 0; i < size; i++) {
                assertThat(iterator.hasNext(), equalTo(true));
                Entry<BytesRef, String> next = iterator.next();
                assertThat("pos: " + i, searchResponse.getHits().getAt(i).getId(), equalTo(next.getValue()));
                assertThat(searchResponse.getHits().getAt(i).getSortValues()[0].toString(), equalTo(next.getKey().utf8ToString()));
            }
        }
        if (!sparseBytes.isEmpty()) {
            int size = between(1, sparseBytes.size());
            SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setPostFilter(QueryBuilders.existsQuery("sparse_bytes"))
                .setSize(size)
                .addSort("sparse_bytes", SortOrder.ASC)
                .get();
            assertNoFailures(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo((long) sparseBytes.size()));
            assertThat(searchResponse.getHits().getHits().length, equalTo(size));
            Set<Entry<BytesRef, String>> entrySet = sparseBytes.entrySet();
            Iterator<Entry<BytesRef, String>> iterator = entrySet.iterator();
            for (int i = 0; i < size; i++) {
                assertThat(iterator.hasNext(), equalTo(true));
                Entry<BytesRef, String> next = iterator.next();
                assertThat(searchResponse.getHits().getAt(i).getId(), equalTo(next.getValue()));
                assertThat(searchResponse.getHits().getAt(i).getSortValues()[0].toString(), equalTo(next.getKey().utf8ToString()));
            }
        }
    }

    public void test3078() {
        assertAcked(client().admin().indices().prepareCreate("test").setMapping("field", "type=keyword").get());
        ensureGreen();

        for (int i = 1; i < 101; i++) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource("field", Integer.toString(i)).get();
        }
        refresh();
        SearchResponse searchResponse = client().prepareSearch("test")
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.fieldSort("field").order(SortOrder.ASC))
            .get();
        assertThat(searchResponse.getHits().getAt(0).getSortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getSortValues()[0].toString(), equalTo("10"));
        assertThat(searchResponse.getHits().getAt(2).getSortValues()[0].toString(), equalTo("100"));

        // reindex and refresh
        client().prepareIndex("test").setId(Integer.toString(1)).setSource("field", Integer.toString(1)).get();
        refresh();

        searchResponse = client().prepareSearch("test")
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.fieldSort("field").order(SortOrder.ASC))
            .get();
        assertThat(searchResponse.getHits().getAt(0).getSortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getSortValues()[0].toString(), equalTo("10"));
        assertThat(searchResponse.getHits().getAt(2).getSortValues()[0].toString(), equalTo("100"));

        // reindex - no refresh
        client().prepareIndex("test").setId(Integer.toString(1)).setSource("field", Integer.toString(1)).get();

        searchResponse = client().prepareSearch("test")
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.fieldSort("field").order(SortOrder.ASC))
            .get();
        assertThat(searchResponse.getHits().getAt(0).getSortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getSortValues()[0].toString(), equalTo("10"));
        assertThat(searchResponse.getHits().getAt(2).getSortValues()[0].toString(), equalTo("100"));

        // force merge
        forceMerge();
        refresh();

        client().prepareIndex("test").setId(Integer.toString(1)).setSource("field", Integer.toString(1)).get();
        searchResponse = client().prepareSearch("test")
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.fieldSort("field").order(SortOrder.ASC))
            .get();
        assertThat(searchResponse.getHits().getAt(0).getSortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getSortValues()[0].toString(), equalTo("10"));
        assertThat(searchResponse.getHits().getAt(2).getSortValues()[0].toString(), equalTo("100"));

        refresh();
        searchResponse = client().prepareSearch("test")
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.fieldSort("field").order(SortOrder.ASC))
            .get();
        assertThat(searchResponse.getHits().getAt(0).getSortValues()[0].toString(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getSortValues()[0].toString(), equalTo("10"));
        assertThat(searchResponse.getHits().getAt(2).getSortValues()[0].toString(), equalTo("100"));
    }

    public void testScoreSortDirection() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test").setId("1").setSource("field", 2).get();
        client().prepareIndex("test").setId("2").setSource("field", 1).get();
        client().prepareIndex("test").setId("3").setSource("field", 0).get();

        refresh();

        SearchResponse searchResponse = client().prepareSearch("test")
            .setQuery(QueryBuilders.functionScoreQuery(matchAllQuery(), ScoreFunctionBuilders.fieldValueFactorFunction("field")))
            .get();
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getScore(), Matchers.lessThan(searchResponse.getHits().getAt(0).getScore()));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(2).getScore(), Matchers.lessThan(searchResponse.getHits().getAt(1).getScore()));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));

        searchResponse = client().prepareSearch("test")
            .setQuery(QueryBuilders.functionScoreQuery(matchAllQuery(), ScoreFunctionBuilders.fieldValueFactorFunction("field")))
            .addSort("_score", SortOrder.DESC)
            .get();
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getScore(), Matchers.lessThan(searchResponse.getHits().getAt(0).getScore()));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(2).getScore(), Matchers.lessThan(searchResponse.getHits().getAt(1).getScore()));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));

        searchResponse = client().prepareSearch("test")
            .setQuery(QueryBuilders.functionScoreQuery(matchAllQuery(), ScoreFunctionBuilders.fieldValueFactorFunction("field")))
            .addSort("_score", SortOrder.DESC)
            .get();
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
    }

    public void testScoreSortDirectionWithFunctionScore() throws Exception {
        createIndex("test");
        ensureGreen();

        client().prepareIndex("test").setId("1").setSource("field", 2).get();
        client().prepareIndex("test").setId("2").setSource("field", 1).get();
        client().prepareIndex("test").setId("3").setSource("field", 0).get();

        refresh();

        SearchResponse searchResponse = client().prepareSearch("test")
            .setQuery(functionScoreQuery(matchAllQuery(), fieldValueFactorFunction("field")))
            .get();
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getScore(), Matchers.lessThan(searchResponse.getHits().getAt(0).getScore()));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(2).getScore(), Matchers.lessThan(searchResponse.getHits().getAt(1).getScore()));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));

        searchResponse = client().prepareSearch("test")
            .setQuery(functionScoreQuery(matchAllQuery(), fieldValueFactorFunction("field")))
            .addSort("_score", SortOrder.DESC)
            .get();
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getScore(), Matchers.lessThan(searchResponse.getHits().getAt(0).getScore()));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(2).getScore(), Matchers.lessThan(searchResponse.getHits().getAt(1).getScore()));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));

        searchResponse = client().prepareSearch("test")
            .setQuery(functionScoreQuery(matchAllQuery(), fieldValueFactorFunction("field")))
            .addSort("_score", SortOrder.DESC)
            .get();
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
    }

    public void testIssue2986() {
        assertAcked(client().admin().indices().prepareCreate("test").setMapping("field1", "type=keyword").get());

        client().prepareIndex("test").setId("1").setSource("{\"field1\":\"value1\"}", XContentType.JSON).get();
        client().prepareIndex("test").setId("2").setSource("{\"field1\":\"value2\"}", XContentType.JSON).get();
        client().prepareIndex("test").setId("3").setSource("{\"field1\":\"value3\"}", XContentType.JSON).get();
        refresh();
        SearchResponse result = client().prepareSearch("test")
            .setQuery(matchAllQuery())
            .setTrackScores(true)
            .addSort("field1", SortOrder.ASC)
            .get();

        for (SearchHit hit : result.getHits()) {
            assertFalse(Float.isNaN(hit.getScore()));
        }
    }

    public void testIssue2991() {
        for (int i = 1; i < 4; i++) {
            try {
                client().admin().indices().prepareDelete("test").get();
            } catch (Exception e) {
                // ignore
            }
            assertAcked(client().admin().indices().prepareCreate("test").setMapping("tag", "type=keyword").get());
            ensureGreen();
            client().prepareIndex("test").setId("1").setSource("tag", "alpha").get();
            refresh();

            client().prepareIndex("test").setId("3").setSource("tag", "gamma").get();
            refresh();

            client().prepareIndex("test").setId("4").setSource("tag", "delta").get();

            refresh();
            client().prepareIndex("test").setId("2").setSource("tag", "beta").get();

            refresh();
            SearchResponse resp = client().prepareSearch("test")
                .setSize(2)
                .setQuery(matchAllQuery())
                .addSort(SortBuilders.fieldSort("tag").order(SortOrder.ASC))
                .get();
            assertHitCount(resp, 4);
            assertThat(resp.getHits().getHits().length, equalTo(2));
            assertFirstHit(resp, hasId("1"));
            assertSecondHit(resp, hasId("2"));

            resp = client().prepareSearch("test")
                .setSize(2)
                .setQuery(matchAllQuery())
                .addSort(SortBuilders.fieldSort("tag").order(SortOrder.DESC))
                .get();
            assertHitCount(resp, 4);
            assertThat(resp.getHits().getHits().length, equalTo(2));
            assertFirstHit(resp, hasId("3"));
            assertSecondHit(resp, hasId("4"));
        }
    }

    public void testSimpleSorts() throws Exception {
        Random random = random();
        assertAcked(
            prepareCreate("test").setMapping(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject("str_value")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("boolean_value")
                    .field("type", "boolean")
                    .endObject()
                    .startObject("byte_value")
                    .field("type", "byte")
                    .endObject()
                    .startObject("short_value")
                    .field("type", "short")
                    .endObject()
                    .startObject("integer_value")
                    .field("type", "integer")
                    .endObject()
                    .startObject("long_value")
                    .field("type", "long")
                    .endObject()
                    .startObject("unsigned_long_value")
                    .field("type", "unsigned_long")
                    .endObject()
                    .startObject("float_value")
                    .field("type", "float")
                    .endObject()
                    .startObject("double_value")
                    .field("type", "double")
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        ensureGreen();
        BigInteger UNSIGNED_LONG_BASE = Numbers.MAX_UNSIGNED_LONG_VALUE.subtract(BigInteger.valueOf(100000));
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            IndexRequestBuilder builder = client().prepareIndex("test")
                .setId(Integer.toString(i))
                .setSource(
                    jsonBuilder().startObject()
                        .field("str_value", new String(new char[] { (char) (97 + i), (char) (97 + i) }))
                        .field("boolean_value", true)
                        .field("byte_value", i)
                        .field("short_value", i)
                        .field("integer_value", i)
                        .field("long_value", i)
                        .field("unsigned_long_value", UNSIGNED_LONG_BASE.add(BigInteger.valueOf(10000 * i)))
                        .field("float_value", 0.1 * i)
                        .field("double_value", 0.1 * i)
                        .endObject()
                );
            builders.add(builder);
        }
        Collections.shuffle(builders, random);
        for (IndexRequestBuilder builder : builders) {
            builder.get();
            if (random.nextBoolean()) {
                if (random.nextInt(5) != 0) {
                    refresh();
                } else {
                    client().admin().indices().prepareFlush().get();
                }
            }

        }
        refresh();

        // STRING
        int size = 1 + random.nextInt(10);
        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .setSize(size)
            .addSort("str_value", SortOrder.ASC)
            .get();
        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().getHits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).getId(), equalTo(Integer.toString(i)));
            assertThat(
                searchResponse.getHits().getAt(i).getSortValues()[0].toString(),
                equalTo(new String(new char[] { (char) (97 + i), (char) (97 + i) }))
            );
        }
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("str_value", SortOrder.DESC).get();

        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().getHits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).getId(), equalTo(Integer.toString(9 - i)));
            assertThat(
                searchResponse.getHits().getAt(i).getSortValues()[0].toString(),
                equalTo(new String(new char[] { (char) (97 + (9 - i)), (char) (97 + (9 - i)) }))
            );
        }

        assertThat(searchResponse.toString(), not(containsString("error")));

        // BYTE
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("byte_value", SortOrder.ASC).get();

        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().getHits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).getId(), equalTo(Integer.toString(i)));
            assertThat(((Number) searchResponse.getHits().getAt(i).getSortValues()[0]).byteValue(), equalTo((byte) i));
        }
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("byte_value", SortOrder.DESC).get();

        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().getHits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).getId(), equalTo(Integer.toString(9 - i)));
            assertThat(((Number) searchResponse.getHits().getAt(i).getSortValues()[0]).byteValue(), equalTo((byte) (9 - i)));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));

        // SHORT
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("short_value", SortOrder.ASC).get();

        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().getHits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).getId(), equalTo(Integer.toString(i)));
            assertThat(((Number) searchResponse.getHits().getAt(i).getSortValues()[0]).shortValue(), equalTo((short) i));
        }
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("short_value", SortOrder.DESC).get();

        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().getHits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).getId(), equalTo(Integer.toString(9 - i)));
            assertThat(((Number) searchResponse.getHits().getAt(i).getSortValues()[0]).shortValue(), equalTo((short) (9 - i)));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));

        // INTEGER
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("integer_value", SortOrder.ASC).get();

        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().getHits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).getId(), equalTo(Integer.toString(i)));
            assertThat(((Number) searchResponse.getHits().getAt(i).getSortValues()[0]).intValue(), equalTo(i));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("integer_value", SortOrder.DESC).get();

        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().getHits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).getId(), equalTo(Integer.toString(9 - i)));
            assertThat(((Number) searchResponse.getHits().getAt(i).getSortValues()[0]).intValue(), equalTo((9 - i)));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));

        // LONG
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("long_value", SortOrder.ASC).get();

        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().getHits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).getId(), equalTo(Integer.toString(i)));
            assertThat(((Number) searchResponse.getHits().getAt(i).getSortValues()[0]).longValue(), equalTo((long) i));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("long_value", SortOrder.DESC).get();
        assertHitCount(searchResponse, 10L);
        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().getHits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).getId(), equalTo(Integer.toString(9 - i)));
            assertThat(((Number) searchResponse.getHits().getAt(i).getSortValues()[0]).longValue(), equalTo((long) (9 - i)));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));

        // FLOAT
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("float_value", SortOrder.ASC).get();

        assertHitCount(searchResponse, 10L);
        assertThat(searchResponse.getHits().getHits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).getId(), equalTo(Integer.toString(i)));
            assertThat(((Number) searchResponse.getHits().getAt(i).getSortValues()[0]).doubleValue(), closeTo(0.1d * i, 0.000001d));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("float_value", SortOrder.DESC).get();

        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().getHits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).getId(), equalTo(Integer.toString(9 - i)));
            assertThat(((Number) searchResponse.getHits().getAt(i).getSortValues()[0]).doubleValue(), closeTo(0.1d * (9 - i), 0.000001d));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));

        // DOUBLE
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("double_value", SortOrder.ASC).get();

        assertHitCount(searchResponse, 10L);
        assertThat(searchResponse.getHits().getHits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).getId(), equalTo(Integer.toString(i)));
            assertThat(((Number) searchResponse.getHits().getAt(i).getSortValues()[0]).doubleValue(), closeTo(0.1d * i, 0.000001d));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("double_value", SortOrder.DESC).get();

        assertHitCount(searchResponse, 10L);
        assertThat(searchResponse.getHits().getHits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).getId(), equalTo(Integer.toString(9 - i)));
            assertThat(((Number) searchResponse.getHits().getAt(i).getSortValues()[0]).doubleValue(), closeTo(0.1d * (9 - i), 0.000001d));
        }

        assertNoFailures(searchResponse);

        // UNSIGNED_LONG
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .setSize(size)
            .addSort("unsigned_long_value", SortOrder.ASC)
            .get();

        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().getHits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).getId(), equalTo(Integer.toString(i)));
            assertThat(
                ((Number) searchResponse.getHits().getAt(i).getSortValues()[0]),
                equalTo(UNSIGNED_LONG_BASE.add(BigInteger.valueOf(10000 * i)))
            );
        }

        assertThat(searchResponse.toString(), not(containsString("error")));
        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .setSize(size)
            .addSort("unsigned_long_value", SortOrder.DESC)
            .get();
        assertHitCount(searchResponse, 10L);
        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().getHits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            assertThat(searchResponse.getHits().getAt(i).getId(), equalTo(Integer.toString(9 - i)));
            assertThat(
                ((Number) searchResponse.getHits().getAt(i).getSortValues()[0]),
                equalTo(UNSIGNED_LONG_BASE.add(BigInteger.valueOf(10000 * (9 - i))))
            );
        }

        assertThat(searchResponse.toString(), not(containsString("error")));
        assertNoFailures(searchResponse);
    }

    public void testSortMissingNumbers() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject("i_value")
                    .field("type", "integer")
                    .endObject()
                    .startObject("d_value")
                    .field("type", "float")
                    .endObject()
                    .startObject("u_value")
                    .field("type", "unsigned_long")
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        ensureGreen();
        client().prepareIndex("test")
            .setId("1")
            .setSource(
                jsonBuilder().startObject().field("id", "1").field("i_value", -1).field("d_value", -1.1).field("u_value", 1).endObject()
            )
            .get();

        client().prepareIndex("test").setId("2").setSource(jsonBuilder().startObject().field("id", "2").endObject()).get();

        client().prepareIndex("test")
            .setId("3")
            .setSource(
                jsonBuilder().startObject().field("id", "3").field("i_value", 2).field("d_value", 2.2).field("u_value", 2).endObject()
            )
            .get();

        flush();
        refresh();

        // DOUBLE
        logger.info("--> sort with no missing (same as missing _last)");
        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.fieldSort("i_value").order(SortOrder.ASC))
            .get();
        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("2"));

        logger.info("--> sort with missing _last");
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.fieldSort("i_value").order(SortOrder.ASC).missing("_last"))
            .get();
        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("2"));

        logger.info("--> sort with missing _first");
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.fieldSort("i_value").order(SortOrder.ASC).missing("_first"))
            .get();
        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));

        // FLOAT
        logger.info("--> sort with no missing (same as missing _last)");
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.fieldSort("d_value").order(SortOrder.ASC))
            .get();
        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("2"));

        logger.info("--> sort with missing _last");
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.fieldSort("d_value").order(SortOrder.ASC).missing("_last"))
            .get();
        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("2"));

        logger.info("--> sort with missing _first");
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.fieldSort("d_value").order(SortOrder.ASC).missing("_first"))
            .get();
        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));

        // UNSIGNED_LONG
        logger.info("--> sort with no missing (same as missing _last)");
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.fieldSort("u_value").order(SortOrder.ASC))
            .get();
        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("2"));

        logger.info("--> sort with missing _last");
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.fieldSort("u_value").order(SortOrder.ASC).missing("_last"))
            .get();
        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("2"));

        logger.info("--> sort with missing _first");
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.fieldSort("u_value").order(SortOrder.ASC).missing("_first"))
            .get();
        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));
    }

    public void testSortMissingNumbersMinMax() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject("l_value")
                    .field("type", "long")
                    .endObject()
                    .startObject("d_value")
                    .field("type", "float")
                    .endObject()
                    .startObject("u_value")
                    .field("type", "unsigned_long")
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        ensureGreen();
        client().prepareIndex("test")
            .setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field("id", "1")
                    .field("l_value", Long.MIN_VALUE)
                    .field("d_value", Float.MIN_VALUE)
                    .field("u_value", Numbers.MIN_UNSIGNED_LONG_VALUE)
                    .endObject()
            )
            .get();

        client().prepareIndex("test").setId("2").setSource(jsonBuilder().startObject().field("id", "2").endObject()).get();

        client().prepareIndex("test")
            .setId("3")
            .setSource(
                jsonBuilder().startObject()
                    .field("id", "3")
                    .field("l_value", Long.MAX_VALUE)
                    .field("d_value", Float.MAX_VALUE)
                    .field("u_value", Numbers.MAX_UNSIGNED_LONG_VALUE)
                    .endObject()
            )
            .get();

        flush();
        refresh();

        // LONG
        logger.info("--> sort with no missing (same as missing _last)");
        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.fieldSort("l_value").order(SortOrder.ASC))
            .get();
        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        // The order here could be unstable (depends on document order) since missing == field value
        assertThat(searchResponse.getHits().getAt(1).getId(), is(oneOf("3", "2")));
        assertThat(searchResponse.getHits().getAt(2).getId(), is(oneOf("2", "3")));

        logger.info("--> sort with missing _last");
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.fieldSort("l_value").order(SortOrder.ASC).missing("_last"))
            .get();
        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        // The order here could be unstable (depends on document order) since missing == field value
        assertThat(searchResponse.getHits().getAt(1).getId(), is(oneOf("3", "2")));
        assertThat(searchResponse.getHits().getAt(2).getId(), is(oneOf("2", "3")));

        logger.info("--> sort with missing _first");
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.fieldSort("l_value").order(SortOrder.ASC).missing("_first"))
            .get();
        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        // The order here could be unstable (depends on document order) since missing == field value
        assertThat(searchResponse.getHits().getAt(0).getId(), is(oneOf("2", "1")));
        assertThat(searchResponse.getHits().getAt(1).getId(), is(oneOf("1", "2")));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));

        // FLOAT
        logger.info("--> sort with no missing (same as missing _last)");
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.fieldSort("d_value").order(SortOrder.ASC))
            .get();
        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("2"));

        logger.info("--> sort with missing _last");
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.fieldSort("d_value").order(SortOrder.ASC).missing("_last"))
            .get();
        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("2"));

        logger.info("--> sort with missing _first");
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.fieldSort("d_value").order(SortOrder.ASC).missing("_first"))
            .get();
        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));

        // UNSIGNED_LONG
        logger.info("--> sort with no missing (same as missing _last)");
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.fieldSort("u_value").order(SortOrder.ASC))
            .get();
        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        // The order here could be unstable (depends on document order) since missing == field value
        assertThat(searchResponse.getHits().getAt(1).getId(), is(oneOf("3", "2")));
        assertThat(searchResponse.getHits().getAt(2).getId(), is(oneOf("2", "3")));

        logger.info("--> sort with missing _last");
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.fieldSort("u_value").order(SortOrder.ASC).missing("_last"))
            .get();
        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        // The order here could be unstable (depends on document order) since missing == field value
        assertThat(searchResponse.getHits().getAt(1).getId(), is(oneOf("3", "2")));
        assertThat(searchResponse.getHits().getAt(2).getId(), is(oneOf("2", "3")));

        logger.info("--> sort with missing _first");
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.fieldSort("u_value").order(SortOrder.ASC).missing("_first"))
            .get();
        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        // The order here could be unstable (depends on document order) since missing == field value
        assertThat(searchResponse.getHits().getAt(0).getId(), is(oneOf("2", "1")));
        assertThat(searchResponse.getHits().getAt(1).getId(), is(oneOf("1", "2")));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));
    }

    public void testSortMissingStrings() throws IOException {
        assertAcked(
            prepareCreate("test").setMapping(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject("value")
                    .field("type", "keyword")
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        ensureGreen();
        client().prepareIndex("test")
            .setId("1")
            .setSource(jsonBuilder().startObject().field("id", "1").field("value", "a").endObject())
            .get();

        client().prepareIndex("test").setId("2").setSource(jsonBuilder().startObject().field("id", "2").endObject()).get();

        client().prepareIndex("test")
            .setId("3")
            .setSource(jsonBuilder().startObject().field("id", "1").field("value", "c").endObject())
            .get();

        flush();
        refresh();

        // TODO: WTF?
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            throw new RuntimeException();
        }

        logger.info("--> sort with no missing (same as missing _last)");
        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.fieldSort("value").order(SortOrder.ASC))
            .get();
        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("2"));

        logger.info("--> sort with missing _last");
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.fieldSort("value").order(SortOrder.ASC).missing("_last"))
            .get();
        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("2"));

        logger.info("--> sort with missing _first");
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.fieldSort("value").order(SortOrder.ASC).missing("_first"))
            .get();
        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));

        logger.info("--> sort with missing b");
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.fieldSort("value").order(SortOrder.ASC).missing("b"))
            .get();
        assertThat(Arrays.toString(searchResponse.getShardFailures()), searchResponse.getFailedShards(), equalTo(0));

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo("2"));
        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo("3"));
    }

    public void testIgnoreUnmapped() throws Exception {
        createIndex("test");

        client().prepareIndex("test")
            .setId("1")
            .setSource(jsonBuilder().startObject().field("id", "1").field("i_value", -1).field("d_value", -1.1).endObject())
            .get();

        logger.info("--> sort with an unmapped field, verify it fails");
        try {
            SearchResponse result = client().prepareSearch().setQuery(matchAllQuery()).addSort(SortBuilders.fieldSort("kkk")).get();
            assertThat("Expected exception but returned with", result, nullValue());
        } catch (SearchPhaseExecutionException e) {
            // we check that it's a parse failure rather than a different shard failure
            for (ShardSearchFailure shardSearchFailure : e.shardFailures()) {
                assertThat(shardSearchFailure.toString(), containsString("[No mapping found for [kkk] in order to sort on]"));
            }
        }

        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.fieldSort("kkk").unmappedType("keyword"))
            .get();
        assertNoFailures(searchResponse);

        // nested field
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(
                SortBuilders.fieldSort("nested.foo")
                    .unmappedType("keyword")
                    .setNestedSort(new NestedSortBuilder("nested").setNestedSort(new NestedSortBuilder("nested.foo")))
            )
            .get();
        assertNoFailures(searchResponse);

        // nestedQuery
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(
                SortBuilders.fieldSort("nested.foo")
                    .unmappedType("keyword")
                    .setNestedSort(new NestedSortBuilder("nested").setFilter(QueryBuilders.termQuery("nested.foo", "abc")))
            )
            .get();
        assertNoFailures(searchResponse);
    }

    public void testSortMVField() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject("long_values")
                    .field("type", "long")
                    .endObject()
                    .startObject("int_values")
                    .field("type", "integer")
                    .endObject()
                    .startObject("short_values")
                    .field("type", "short")
                    .endObject()
                    .startObject("byte_values")
                    .field("type", "byte")
                    .endObject()
                    .startObject("float_values")
                    .field("type", "float")
                    .endObject()
                    .startObject("double_values")
                    .field("type", "double")
                    .endObject()
                    .startObject("string_values")
                    .field("type", "keyword")
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        ensureGreen();

        client().prepareIndex("test")
            .setId(Integer.toString(1))
            .setSource(
                jsonBuilder().startObject()
                    .array("long_values", 1L, 5L, 10L, 8L)
                    .array("int_values", 1, 5, 10, 8)
                    .array("short_values", 1, 5, 10, 8)
                    .array("byte_values", 1, 5, 10, 8)
                    .array("float_values", 1f, 5f, 10f, 8f)
                    .array("double_values", 1d, 5d, 10d, 8d)
                    .array("string_values", "01", "05", "10", "08")
                    .endObject()
            )
            .get();
        client().prepareIndex("test")
            .setId(Integer.toString(2))
            .setSource(
                jsonBuilder().startObject()
                    .array("long_values", 11L, 15L, 20L, 7L)
                    .array("int_values", 11, 15, 20, 7)
                    .array("short_values", 11, 15, 20, 7)
                    .array("byte_values", 11, 15, 20, 7)
                    .array("float_values", 11f, 15f, 20f, 7f)
                    .array("double_values", 11d, 15d, 20d, 7d)
                    .array("string_values", "11", "15", "20", "07")
                    .endObject()
            )
            .get();
        client().prepareIndex("test")
            .setId(Integer.toString(3))
            .setSource(
                jsonBuilder().startObject()
                    .array("long_values", 2L, 1L, 3L, -4L)
                    .array("int_values", 2, 1, 3, -4)
                    .array("short_values", 2, 1, 3, -4)
                    .array("byte_values", 2, 1, 3, -4)
                    .array("float_values", 2f, 1f, 3f, -4f)
                    .array("double_values", 2d, 1d, 3d, -4d)
                    .array("string_values", "02", "01", "03", "!4")
                    .endObject()
            )
            .get();

        refresh();

        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .setSize(10)
            .addSort("long_values", SortOrder.ASC)
            .get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).longValue(), equalTo(-4L));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).longValue(), equalTo(1L));

        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).longValue(), equalTo(7L));

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(10).addSort("long_values", SortOrder.DESC).get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).longValue(), equalTo(20L));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).longValue(), equalTo(10L));

        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).longValue(), equalTo(3L));

        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .setSize(10)
            .addSort(SortBuilders.fieldSort("long_values").order(SortOrder.DESC).sortMode(SortMode.SUM))
            .get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).longValue(), equalTo(53L));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).longValue(), equalTo(24L));

        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).longValue(), equalTo(2L));

        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .setSize(10)
            .addSort(SortBuilders.fieldSort("long_values").order(SortOrder.DESC).sortMode(SortMode.AVG))
            .get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).longValue(), equalTo(13L));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).longValue(), equalTo(6L));

        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).longValue(), equalTo(1L));

        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .setSize(10)
            .addSort(SortBuilders.fieldSort("long_values").order(SortOrder.DESC).sortMode(SortMode.MEDIAN))
            .get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).longValue(), equalTo(13L));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).longValue(), equalTo(7L));

        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).longValue(), equalTo(2L));

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(10).addSort("int_values", SortOrder.ASC).get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).intValue(), equalTo(-4));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).intValue(), equalTo(1));

        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).intValue(), equalTo(7));

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(10).addSort("int_values", SortOrder.DESC).get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).intValue(), equalTo(20));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).intValue(), equalTo(10));

        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).intValue(), equalTo(3));

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(10).addSort("short_values", SortOrder.ASC).get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).intValue(), equalTo(-4));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).intValue(), equalTo(1));

        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).intValue(), equalTo(7));

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(10).addSort("short_values", SortOrder.DESC).get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).intValue(), equalTo(20));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).intValue(), equalTo(10));

        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).intValue(), equalTo(3));

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(10).addSort("byte_values", SortOrder.ASC).get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).intValue(), equalTo(-4));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).intValue(), equalTo(1));

        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).intValue(), equalTo(7));

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(10).addSort("byte_values", SortOrder.DESC).get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).intValue(), equalTo(20));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).intValue(), equalTo(10));

        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).intValue(), equalTo(3));

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(10).addSort("float_values", SortOrder.ASC).get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).floatValue(), equalTo(-4f));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).floatValue(), equalTo(1f));

        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).floatValue(), equalTo(7f));

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(10).addSort("float_values", SortOrder.DESC).get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).floatValue(), equalTo(20f));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).floatValue(), equalTo(10f));

        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).floatValue(), equalTo(3f));

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(10).addSort("double_values", SortOrder.ASC).get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).doubleValue(), equalTo(-4d));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).doubleValue(), equalTo(1d));

        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).doubleValue(), equalTo(7d));

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(10).addSort("double_values", SortOrder.DESC).get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(2)));
        assertThat(((Number) searchResponse.getHits().getAt(0).getSortValues()[0]).doubleValue(), equalTo(20d));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
        assertThat(((Number) searchResponse.getHits().getAt(1).getSortValues()[0]).doubleValue(), equalTo(10d));

        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo(Integer.toString(3)));
        assertThat(((Number) searchResponse.getHits().getAt(2).getSortValues()[0]).doubleValue(), equalTo(3d));

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(10).addSort("string_values", SortOrder.ASC).get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(3)));
        assertThat(searchResponse.getHits().getAt(0).getSortValues()[0], equalTo("!4"));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
        assertThat(searchResponse.getHits().getAt(1).getSortValues()[0], equalTo("01"));

        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo(Integer.toString(2)));
        assertThat(searchResponse.getHits().getAt(2).getSortValues()[0], equalTo("07"));

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(10).addSort("string_values", SortOrder.DESC).get();

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getHits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(2)));
        assertThat(searchResponse.getHits().getAt(0).getSortValues()[0], equalTo("20"));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
        assertThat(searchResponse.getHits().getAt(1).getSortValues()[0], equalTo("10"));

        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo(Integer.toString(3)));
        assertThat(searchResponse.getHits().getAt(2).getSortValues()[0], equalTo("03"));
    }

    public void testSortOnRareField() throws IOException {
        assertAcked(
            prepareCreate("test").setMapping(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject("string_values")
                    .field("type", "keyword")
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        ensureGreen();
        client().prepareIndex("test")
            .setId(Integer.toString(1))
            .setSource(jsonBuilder().startObject().array("string_values", "01", "05", "10", "08").endObject())
            .get();

        refresh();
        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .setSize(3)
            .addSort("string_values", SortOrder.DESC)
            .get();

        assertThat(searchResponse.getHits().getHits().length, equalTo(1));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(1)));
        assertThat(searchResponse.getHits().getAt(0).getSortValues()[0], equalTo("10"));

        client().prepareIndex("test")
            .setId(Integer.toString(2))
            .setSource(jsonBuilder().startObject().array("string_values", "11", "15", "20", "07").endObject())
            .get();
        for (int i = 0; i < 15; i++) {
            client().prepareIndex("test")
                .setId(Integer.toString(300 + i))
                .setSource(jsonBuilder().startObject().array("some_other_field", "foobar").endObject())
                .get();
        }
        refresh();

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(2).addSort("string_values", SortOrder.DESC).get();

        assertThat(searchResponse.getHits().getHits().length, equalTo(2));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(2)));
        assertThat(searchResponse.getHits().getAt(0).getSortValues()[0], equalTo("20"));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
        assertThat(searchResponse.getHits().getAt(1).getSortValues()[0], equalTo("10"));

        client().prepareIndex("test")
            .setId(Integer.toString(3))
            .setSource(jsonBuilder().startObject().array("string_values", "02", "01", "03", "!4").endObject())
            .get();
        for (int i = 0; i < 15; i++) {
            client().prepareIndex("test")
                .setId(Integer.toString(300 + i))
                .setSource(jsonBuilder().startObject().array("some_other_field", "foobar").endObject())
                .get();
        }
        refresh();

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(3).addSort("string_values", SortOrder.DESC).get();

        assertThat(searchResponse.getHits().getHits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(2)));
        assertThat(searchResponse.getHits().getAt(0).getSortValues()[0], equalTo("20"));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
        assertThat(searchResponse.getHits().getAt(1).getSortValues()[0], equalTo("10"));

        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo(Integer.toString(3)));
        assertThat(searchResponse.getHits().getAt(2).getSortValues()[0], equalTo("03"));

        for (int i = 0; i < 15; i++) {
            client().prepareIndex("test")
                .setId(Integer.toString(300 + i))
                .setSource(jsonBuilder().startObject().array("some_other_field", "foobar").endObject())
                .get();
            refresh();
        }

        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(3).addSort("string_values", SortOrder.DESC).get();

        assertThat(searchResponse.getHits().getHits().length, equalTo(3));

        assertThat(searchResponse.getHits().getAt(0).getId(), equalTo(Integer.toString(2)));
        assertThat(searchResponse.getHits().getAt(0).getSortValues()[0], equalTo("20"));

        assertThat(searchResponse.getHits().getAt(1).getId(), equalTo(Integer.toString(1)));
        assertThat(searchResponse.getHits().getAt(1).getSortValues()[0], equalTo("10"));

        assertThat(searchResponse.getHits().getAt(2).getId(), equalTo(Integer.toString(3)));
        assertThat(searchResponse.getHits().getAt(2).getSortValues()[0], equalTo("03"));
    }

    public void testSortMetaField() throws Exception {
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(IndicesService.INDICES_ID_FIELD_DATA_ENABLED_SETTING.getKey(), true))
            .get();
        try {
            createIndex("test");
            ensureGreen();
            final int numDocs = randomIntBetween(10, 20);
            IndexRequestBuilder[] indexReqs = new IndexRequestBuilder[numDocs];
            for (int i = 0; i < numDocs; ++i) {
                indexReqs[i] = client().prepareIndex("test").setId(Integer.toString(i)).setSource();
            }
            indexRandom(true, indexReqs);

            SortOrder order = randomFrom(SortOrder.values());
            SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(randomIntBetween(1, numDocs + 5))
                .addSort("_id", order)
                .get();
            assertNoFailures(searchResponse);
            SearchHit[] hits = searchResponse.getHits().getHits();
            BytesRef previous = order == SortOrder.ASC ? new BytesRef() : UnicodeUtil.BIG_TERM;
            for (int i = 0; i < hits.length; ++i) {
                String idString = hits[i].getId();
                final BytesRef id = new BytesRef(idString);
                assertEquals(idString, hits[i].getSortValues()[0]);
                assertThat(previous, order == SortOrder.ASC ? lessThan(id) : greaterThan(id));
                previous = id;
            }
            // assertWarnings(ID_FIELD_DATA_DEPRECATION_MESSAGE);
        } finally {
            // unset cluster setting
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().putNull(IndicesService.INDICES_ID_FIELD_DATA_ENABLED_SETTING.getKey()))
                .get();
        }
    }

    /**
     * Test case for issue 6150: https://github.com/elastic/elasticsearch/issues/6150
     */
    public void testNestedSort() throws IOException, InterruptedException, ExecutionException {
        assertAcked(
            prepareCreate("test").setMapping(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("properties")
                    .startObject("nested")
                    .field("type", "nested")
                    .startObject("properties")
                    .startObject("foo")
                    .field("type", "text")
                    .field("fielddata", true)
                    .startObject("fields")
                    .startObject("sub")
                    .field("type", "keyword")
                    .endObject()
                    .endObject()
                    .endObject()
                    .startObject("bar")
                    .field("type", "nested")
                    .startObject("properties")
                    .startObject("foo")
                    .field("type", "text")
                    .field("fielddata", true)
                    .startObject("fields")
                    .startObject("sub")
                    .field("type", "keyword")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        ensureGreen();

        client().prepareIndex("test")
            .setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .startArray("nested")
                    .startObject()
                    .field("foo", "bar bar")
                    .endObject()
                    .startObject()
                    .field("foo", "abc abc")
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();
        client().prepareIndex("test")
            .setId("2")
            .setSource(
                jsonBuilder().startObject()
                    .startArray("nested")
                    .startObject()
                    .field("foo", "abc abc")
                    .endObject()
                    .startObject()
                    .field("foo", "cba bca")
                    .endObject()
                    .endArray()
                    .endObject()
            )
            .get();
        refresh();

        // We sort on nested field
        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.fieldSort("nested.foo").setNestedPath("nested").order(SortOrder.DESC))
            .get();
        assertNoFailures(searchResponse);
        SearchHit[] hits = searchResponse.getHits().getHits();
        assertThat(hits.length, is(2));
        assertThat(hits[0].getSortValues().length, is(1));
        assertThat(hits[1].getSortValues().length, is(1));
        assertThat(hits[0].getSortValues()[0], is("cba"));
        assertThat(hits[1].getSortValues()[0], is("bar"));

        // We sort on nested fields with max_children limit
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(
                SortBuilders.fieldSort("nested.foo").setNestedSort(new NestedSortBuilder("nested").setMaxChildren(1)).order(SortOrder.DESC)
            )
            .get();
        assertNoFailures(searchResponse);
        hits = searchResponse.getHits().getHits();
        assertThat(hits.length, is(2));
        assertThat(hits[0].getSortValues().length, is(1));
        assertThat(hits[1].getSortValues().length, is(1));
        assertThat(hits[0].getSortValues()[0], is("bar"));
        assertThat(hits[1].getSortValues()[0], is("abc"));

        {
            SearchPhaseExecutionException exc = expectThrows(
                SearchPhaseExecutionException.class,
                () -> client().prepareSearch()
                    .setQuery(matchAllQuery())
                    .addSort(
                        SortBuilders.fieldSort("nested.bar.foo")
                            .setNestedSort(
                                new NestedSortBuilder("nested").setNestedSort(new NestedSortBuilder("nested.bar").setMaxChildren(1))
                            )
                            .order(SortOrder.DESC)
                    )
                    .get()
            );
            assertThat(exc.toString(), containsString("max_children is only supported on top level of nested sort"));
        }

        // We sort on nested sub field
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(SortBuilders.fieldSort("nested.foo.sub").setNestedPath("nested").order(SortOrder.DESC))
            .get();
        assertNoFailures(searchResponse);
        hits = searchResponse.getHits().getHits();
        assertThat(hits.length, is(2));
        assertThat(hits[0].getSortValues().length, is(1));
        assertThat(hits[1].getSortValues().length, is(1));
        assertThat(hits[0].getSortValues()[0], is("cba bca"));
        assertThat(hits[1].getSortValues()[0], is("bar bar"));
    }

    public void testSortDuelBetweenSingleShardAndMultiShardIndex() throws Exception {
        String sortField = "sortField";
        assertAcked(
            prepareCreate("test1").setSettings(
                Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, between(2, maximumNumberOfShards()))
            ).setMapping(sortField, "type=long").get()
        );
        assertAcked(
            prepareCreate("test2").setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1))
                .setMapping(sortField, "type=long")
                .get()
        );

        for (String index : new String[] { "test1", "test2" }) {
            List<IndexRequestBuilder> docs = new ArrayList<>();
            for (int i = 0; i < 256; i++) {
                docs.add(client().prepareIndex(index).setId(Integer.toString(i)).setSource(sortField, i));
            }
            indexRandom(true, docs);
        }

        ensureSearchable("test1", "test2");
        SortOrder order = randomBoolean() ? SortOrder.ASC : SortOrder.DESC;
        int from = between(0, 256);
        int size = between(0, 256);
        SearchResponse multiShardResponse = client().prepareSearch("test1").setFrom(from).setSize(size).addSort(sortField, order).get();
        assertNoFailures(multiShardResponse);
        SearchResponse singleShardResponse = client().prepareSearch("test2").setFrom(from).setSize(size).addSort(sortField, order).get();
        assertNoFailures(singleShardResponse);

        assertThat(multiShardResponse.getHits().getTotalHits().value, equalTo(singleShardResponse.getHits().getTotalHits().value));
        assertThat(multiShardResponse.getHits().getHits().length, equalTo(singleShardResponse.getHits().getHits().length));
        for (int i = 0; i < multiShardResponse.getHits().getHits().length; i++) {
            assertThat(
                multiShardResponse.getHits().getAt(i).getSortValues()[0],
                equalTo(singleShardResponse.getHits().getAt(i).getSortValues()[0])
            );
            assertThat(multiShardResponse.getHits().getAt(i).getId(), equalTo(singleShardResponse.getHits().getAt(i).getId()));
        }
    }

    public void testCustomFormat() throws Exception {
        // Use an ip field, which uses different internal/external
        // representations of values, to make sure values are both correctly
        // rendered and parsed (search_after)
        assertAcked(prepareCreate("test").setMapping("ip", "type=ip"));
        indexRandom(
            true,
            client().prepareIndex("test").setId("1").setSource("ip", "192.168.1.7"),
            client().prepareIndex("test").setId("2").setSource("ip", "2001:db8::ff00:42:8329")
        );

        SearchResponse response = client().prepareSearch("test").addSort(SortBuilders.fieldSort("ip")).get();
        assertSearchResponse(response);
        assertEquals(2, response.getHits().getTotalHits().value);
        assertArrayEquals(new String[] { "192.168.1.7" }, response.getHits().getAt(0).getSortValues());
        assertArrayEquals(new String[] { "2001:db8::ff00:42:8329" }, response.getHits().getAt(1).getSortValues());

        response = client().prepareSearch("test").addSort(SortBuilders.fieldSort("ip")).searchAfter(new Object[] { "192.168.1.7" }).get();
        assertSearchResponse(response);
        assertEquals(2, response.getHits().getTotalHits().value);
        assertEquals(1, response.getHits().getHits().length);
        assertArrayEquals(new String[] { "2001:db8::ff00:42:8329" }, response.getHits().getAt(0).getSortValues());
    }

    public void testScriptFieldSort() throws Exception {
        assertAcked(prepareCreate("test").setMapping("keyword", "type=keyword", "number", "type=integer"));
        ensureGreen();
        final int numDocs = randomIntBetween(10, 20);
        IndexRequestBuilder[] indexReqs = new IndexRequestBuilder[numDocs];
        List<String> keywords = new ArrayList<>();
        for (int i = 0; i < numDocs; ++i) {
            indexReqs[i] = client().prepareIndex("test").setSource("number", i, "keyword", Integer.toString(i));
            keywords.add(Integer.toString(i));
        }
        Collections.sort(keywords);
        indexRandom(true, indexReqs);

        {
            Script script = new Script(ScriptType.INLINE, NAME, "doc['number'].value", Collections.emptyMap());
            SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(randomIntBetween(1, numDocs + 5))
                .addSort(SortBuilders.scriptSort(script, ScriptSortBuilder.ScriptSortType.NUMBER))
                .addSort(SortBuilders.scoreSort())
                .get();

            double expectedValue = 0;
            for (SearchHit hit : searchResponse.getHits()) {
                assertThat(hit.getSortValues().length, equalTo(2));
                assertThat(hit.getSortValues()[0], equalTo(expectedValue++));
                assertThat(hit.getSortValues()[1], equalTo(1f));
            }
        }

        {
            Script script = new Script(ScriptType.INLINE, NAME, "doc['keyword'].value", Collections.emptyMap());
            SearchResponse searchResponse = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(randomIntBetween(1, numDocs + 5))
                .addSort(SortBuilders.scriptSort(script, ScriptSortBuilder.ScriptSortType.STRING))
                .addSort(SortBuilders.scoreSort())
                .get();

            int expectedValue = 0;
            for (SearchHit hit : searchResponse.getHits()) {
                assertThat(hit.getSortValues().length, equalTo(2));
                assertThat(hit.getSortValues()[0], equalTo(keywords.get(expectedValue++)));
                assertThat(hit.getSortValues()[1], equalTo(1f));
            }
        }
    }

    public void testFieldAlias() throws Exception {
        // Create two indices and add the field 'route_length_miles' as an alias in
        // one, and a concrete field in the other.
        assertAcked(prepareCreate("old_index").setMapping("distance", "type=double", "route_length_miles", "type=alias,path=distance"));
        assertAcked(prepareCreate("new_index").setMapping("route_length_miles", "type=double"));
        ensureGreen("old_index", "new_index");

        List<IndexRequestBuilder> builders = new ArrayList<>();
        builders.add(client().prepareIndex("old_index").setSource("distance", 42.0));
        builders.add(client().prepareIndex("old_index").setSource("distance", 50.5));
        builders.add(client().prepareIndex("new_index").setSource("route_length_miles", 100.2));
        indexRandom(true, true, builders);

        SearchResponse response = client().prepareSearch()
            .setQuery(matchAllQuery())
            .setSize(builders.size())
            .addSort(SortBuilders.fieldSort("route_length_miles"))
            .get();
        SearchHits hits = response.getHits();

        assertEquals(3, hits.getHits().length);
        assertEquals(42.0, hits.getAt(0).getSortValues()[0]);
        assertEquals(50.5, hits.getAt(1).getSortValues()[0]);
        assertEquals(100.2, hits.getAt(2).getSortValues()[0]);
    }

    public void testFieldAliasesWithMissingValues() throws Exception {
        // Create two indices and add the field 'route_length_miles' as an alias in
        // one, and a concrete field in the other.
        assertAcked(prepareCreate("old_index").setMapping("distance", "type=double", "route_length_miles", "type=alias,path=distance"));
        assertAcked(prepareCreate("new_index").setMapping("route_length_miles", "type=double"));
        ensureGreen("old_index", "new_index");

        List<IndexRequestBuilder> builders = new ArrayList<>();
        builders.add(client().prepareIndex("old_index").setSource("distance", 42.0));
        builders.add(client().prepareIndex("old_index").setSource(Collections.emptyMap()));
        builders.add(client().prepareIndex("new_index").setSource("route_length_miles", 100.2));
        indexRandom(true, true, builders);

        SearchResponse response = client().prepareSearch()
            .setQuery(matchAllQuery())
            .setSize(builders.size())
            .addSort(SortBuilders.fieldSort("route_length_miles").missing(120.3))
            .get();
        SearchHits hits = response.getHits();

        assertEquals(3, hits.getHits().length);
        assertEquals(42.0, hits.getAt(0).getSortValues()[0]);
        assertEquals(100.2, hits.getAt(1).getSortValues()[0]);
        assertEquals(120.3, hits.getAt(2).getSortValues()[0]);
    }

    public void testCastNumericType() throws Exception {
        assertAcked(prepareCreate("index_double").setMapping("field", "type=double"));
        assertAcked(prepareCreate("index_long").setMapping("field", "type=long"));
        assertAcked(prepareCreate("index_float").setMapping("field", "type=float"));
        ensureGreen("index_double", "index_long", "index_float");

        List<IndexRequestBuilder> builders = new ArrayList<>();
        builders.add(client().prepareIndex("index_double").setSource("field", 12.6));
        builders.add(client().prepareIndex("index_long").setSource("field", 12));
        builders.add(client().prepareIndex("index_float").setSource("field", 12.1));
        indexRandom(true, true, builders);

        {
            SearchResponse response = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(builders.size())
                .addSort(SortBuilders.fieldSort("field").setNumericType("long"))
                .get();
            SearchHits hits = response.getHits();

            assertEquals(3, hits.getHits().length);
            for (int i = 0; i < 3; i++) {
                assertThat(hits.getAt(i).getSortValues()[0].getClass(), equalTo(Long.class));
            }
            assertEquals(12L, hits.getAt(0).getSortValues()[0]);
            assertEquals(12L, hits.getAt(1).getSortValues()[0]);
            assertEquals(12L, hits.getAt(2).getSortValues()[0]);
        }

        {
            SearchResponse response = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(builders.size())
                .addSort(SortBuilders.fieldSort("field").setNumericType("double"))
                .get();
            SearchHits hits = response.getHits();
            assertEquals(3, hits.getHits().length);
            for (int i = 0; i < 3; i++) {
                assertThat(hits.getAt(i).getSortValues()[0].getClass(), equalTo(Double.class));
            }
            assertEquals(12D, hits.getAt(0).getSortValues()[0]);
            assertEquals(12.1D, (double) hits.getAt(1).getSortValues()[0], 0.001f);
            assertEquals(12.6D, hits.getAt(2).getSortValues()[0]);
        }
    }

    public void testCastDate() throws Exception {
        assertAcked(prepareCreate("index_date").setMapping("field", "type=date"));
        assertAcked(prepareCreate("index_date_nanos").setMapping("field", "type=date_nanos"));
        ensureGreen("index_date", "index_date_nanos");

        List<IndexRequestBuilder> builders = new ArrayList<>();
        builders.add(client().prepareIndex("index_date").setSource("field", "2024-04-11T23:47:17"));
        builders.add(client().prepareIndex("index_date_nanos").setSource("field", "2024-04-11T23:47:16.854775807Z"));
        indexRandom(true, true, builders);

        {
            SearchResponse response = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(2)
                .addSort(SortBuilders.fieldSort("field").setNumericType("date"))
                .get();
            SearchHits hits = response.getHits();

            assertEquals(2, hits.getHits().length);
            for (int i = 0; i < 2; i++) {
                assertThat(hits.getAt(i).getSortValues()[0].getClass(), equalTo(Long.class));
            }
            assertEquals(1712879236854L, hits.getAt(0).getSortValues()[0]);
            assertEquals(1712879237000L, hits.getAt(1).getSortValues()[0]);

            response = client().prepareSearch()
                .setMaxConcurrentShardRequests(1)
                .setQuery(matchAllQuery())
                .setSize(1)
                .addSort(SortBuilders.fieldSort("field").setNumericType("date"))
                .get();
            hits = response.getHits();

            assertEquals(1, hits.getHits().length);
            assertThat(hits.getAt(0).getSortValues()[0].getClass(), equalTo(Long.class));
            assertEquals(1712879236854L, hits.getAt(0).getSortValues()[0]);

            response = client().prepareSearch()
                .setMaxConcurrentShardRequests(1)
                .setQuery(matchAllQuery())
                .setSize(1)
                .addSort(SortBuilders.fieldSort("field").order(SortOrder.DESC).setNumericType("date"))
                .get();
            hits = response.getHits();

            assertEquals(1, hits.getHits().length);
            assertThat(hits.getAt(0).getSortValues()[0].getClass(), equalTo(Long.class));
            assertEquals(1712879237000L, hits.getAt(0).getSortValues()[0]);
        }

        {
            SearchResponse response = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(2)
                .addSort(SortBuilders.fieldSort("field").setNumericType("date_nanos"))
                .get();
            SearchHits hits = response.getHits();
            assertEquals(2, hits.getHits().length);
            for (int i = 0; i < 2; i++) {
                assertThat(hits.getAt(i).getSortValues()[0].getClass(), equalTo(Long.class));
            }
            assertEquals(1712879236854775807L, hits.getAt(0).getSortValues()[0]);
            assertEquals(1712879237000000000L, hits.getAt(1).getSortValues()[0]);

            response = client().prepareSearch()
                .setMaxConcurrentShardRequests(1)
                .setQuery(matchAllQuery())
                .setSize(1)
                .addSort(SortBuilders.fieldSort("field").setNumericType("date_nanos"))
                .get();
            hits = response.getHits();
            assertEquals(1, hits.getHits().length);
            assertThat(hits.getAt(0).getSortValues()[0].getClass(), equalTo(Long.class));
            assertEquals(1712879236854775807L, hits.getAt(0).getSortValues()[0]);

            response = client().prepareSearch()
                .setMaxConcurrentShardRequests(1)
                .setQuery(matchAllQuery())
                .setSize(1)
                .addSort(SortBuilders.fieldSort("field").order(SortOrder.DESC).setNumericType("date_nanos"))
                .get();
            hits = response.getHits();
            assertEquals(1, hits.getHits().length);
            assertThat(hits.getAt(0).getSortValues()[0].getClass(), equalTo(Long.class));
            assertEquals(1712879237000000000L, hits.getAt(0).getSortValues()[0]);
        }

        {
            builders.clear();
            builders.add(client().prepareIndex("index_date").setSource("field", "1905-04-11T23:47:17"));
            indexRandom(true, true, builders);
            SearchResponse response = client().prepareSearch()
                .setQuery(matchAllQuery())
                .setSize(1)
                .addSort(SortBuilders.fieldSort("field").setNumericType("date_nanos"))
                .get();
            assertNotNull(response.getShardFailures());
            assertThat(response.getShardFailures().length, equalTo(1));
            assertThat(response.getShardFailures()[0].toString(), containsString("are before the epoch in 1970"));
        }

        {
            builders.clear();
            builders.add(client().prepareIndex("index_date").setSource("field", "2346-04-11T23:47:17"));
            indexRandom(true, true, builders);
            SearchResponse response = client().prepareSearch()
                .setQuery(QueryBuilders.rangeQuery("field").gt("1970-01-01"))
                .setSize(10)
                .addSort(SortBuilders.fieldSort("field").setNumericType("date_nanos"))
                .get();
            assertNotNull(response.getShardFailures());
            assertThat(response.getShardFailures().length, equalTo(1));
            assertThat(response.getShardFailures()[0].toString(), containsString("are after 2262"));
        }
    }

    public void testCastNumericTypeExceptions() throws Exception {
        assertAcked(prepareCreate("index").setMapping("keyword", "type=keyword", "ip", "type=ip"));
        ensureGreen("index");
        for (String invalidField : new String[] { "keyword", "ip" }) {
            for (String numericType : new String[] { "long", "double", "date", "date_nanos" }) {
                OpenSearchException exc = expectThrows(
                    OpenSearchException.class,
                    () -> client().prepareSearch()
                        .setQuery(matchAllQuery())
                        .addSort(SortBuilders.fieldSort(invalidField).setNumericType(numericType))
                        .get()
                );
                assertThat(exc.status(), equalTo(RestStatus.BAD_REQUEST));
                assertThat(exc.getDetailedMessage(), containsString("[numeric_type] option cannot be set on a non-numeric field"));
            }
        }
    }

    public void testLongSortOptimizationCorrectResults() {
        assertAcked(
            prepareCreate("test1").setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2))
                .setMapping("long_field", "type=long")
                .get()
        );

        BulkRequestBuilder bulkBuilder = client().prepareBulk();
        for (int i = 1; i <= 7000; i++) {
            if (i % 3500 == 0) {
                bulkBuilder.get();
                bulkBuilder = client().prepareBulk();
            }
            String source = "{\"long_field\":" + randomLong() + "}";
            bulkBuilder.add(client().prepareIndex("test1").setId(Integer.toString(i)).setSource(source, XContentType.JSON));
        }
        refresh();

        // *** 1. sort DESC on long_field
        SearchResponse searchResponse = client().prepareSearch()
            .addSort(new FieldSortBuilder("long_field").order(SortOrder.DESC))
            .setSize(10)
            .get();
        assertSearchResponse(searchResponse);
        long previousLong = Long.MAX_VALUE;
        for (int i = 0; i < searchResponse.getHits().getHits().length; i++) {
            // check the correct sort order
            SearchHit hit = searchResponse.getHits().getHits()[i];
            long currentLong = (long) hit.getSortValues()[0];
            assertThat("sort order is incorrect", currentLong, lessThanOrEqualTo(previousLong));
            previousLong = currentLong;
        }

        // *** 2. sort ASC on long_field
        searchResponse = client().prepareSearch().addSort(new FieldSortBuilder("long_field").order(SortOrder.ASC)).setSize(10).get();
        assertSearchResponse(searchResponse);
        previousLong = Long.MIN_VALUE;
        for (int i = 0; i < searchResponse.getHits().getHits().length; i++) {
            // check the correct sort order
            SearchHit hit = searchResponse.getHits().getHits()[i];
            long currentLong = (long) hit.getSortValues()[0];
            assertThat("sort order is incorrect", currentLong, greaterThanOrEqualTo(previousLong));
            previousLong = currentLong;
        }
    }

}
