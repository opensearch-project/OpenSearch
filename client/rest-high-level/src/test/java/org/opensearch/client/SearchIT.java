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

package org.opensearch.client;

import org.apache.hc.client5.http.classic.methods.HttpPost;
import org.apache.hc.client5.http.classic.methods.HttpPut;
import org.opensearch.OpenSearchException;
import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.explain.ExplainRequest;
import org.opensearch.action.explain.ExplainResponse;
import org.opensearch.action.fieldcaps.FieldCapabilities;
import org.opensearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.opensearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.opensearch.action.search.ClearScrollRequest;
import org.opensearch.action.search.ClearScrollResponse;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.DeletePitRequest;
import org.opensearch.action.search.DeletePitResponse;
import org.opensearch.action.search.MultiSearchRequest;
import org.opensearch.action.search.MultiSearchResponse;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.client.core.CountRequest;
import org.opensearch.client.core.CountResponse;
import org.opensearch.common.geo.ShapeRelation;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.geometry.Rectangle;
import org.opensearch.index.query.GeoShapeQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.ScriptQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.join.aggregations.Children;
import org.opensearch.join.aggregations.ChildrenAggregationBuilder;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.script.mustache.MultiSearchTemplateRequest;
import org.opensearch.script.mustache.MultiSearchTemplateResponse;
import org.opensearch.script.mustache.MultiSearchTemplateResponse.Item;
import org.opensearch.script.mustache.SearchTemplateRequest;
import org.opensearch.script.mustache.SearchTemplateResponse;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.BucketOrder;
import org.opensearch.search.aggregations.bucket.composite.CompositeAggregation;
import org.opensearch.search.aggregations.bucket.composite.CompositeValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.composite.TermsValuesSourceBuilder;
import org.opensearch.search.aggregations.bucket.range.Range;
import org.opensearch.search.aggregations.bucket.range.RangeAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.MultiTermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.RareTerms;
import org.opensearch.search.aggregations.bucket.terms.RareTermsAggregationBuilder;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.opensearch.search.aggregations.matrix.stats.MatrixStats;
import org.opensearch.search.aggregations.matrix.stats.MatrixStatsAggregationBuilder;
import org.opensearch.search.aggregations.metrics.WeightedAvg;
import org.opensearch.search.aggregations.metrics.WeightedAvgAggregationBuilder;
import org.opensearch.search.aggregations.support.MultiTermsValuesSourceConfig;
import org.opensearch.search.aggregations.support.MultiValuesSourceFieldConfig;
import org.opensearch.search.aggregations.support.ValueType;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.search.fetch.subphase.highlight.HighlightBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.search.suggest.Suggest;
import org.opensearch.search.suggest.SuggestBuilder;
import org.opensearch.search.suggest.phrase.PhraseSuggestionBuilder;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.hamcrest.Matchers;
import org.junit.Before;

import java.io.IOException;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.query.QueryBuilders.geoShapeQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.both;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.nullValue;

public class SearchIT extends OpenSearchRestHighLevelClientTestCase {

    @Before
    public void indexDocuments() throws IOException {
        {
            Request doc1 = new Request(HttpPut.METHOD_NAME, "/index/_doc/1");
            doc1.setJsonEntity("{\"type\":\"type1\", \"id\":1, \"num\":10, \"num2\":50}");
            client().performRequest(doc1);
            Request doc2 = new Request(HttpPut.METHOD_NAME, "/index/_doc/2");
            doc2.setJsonEntity("{\"type\":\"type1\", \"id\":2, \"num\":20, \"num2\":40}");
            client().performRequest(doc2);
            Request doc3 = new Request(HttpPut.METHOD_NAME, "/index/_doc/3");
            doc3.setJsonEntity("{\"type\":\"type1\", \"id\":3, \"num\":50, \"num2\":35}");
            client().performRequest(doc3);
            Request doc4 = new Request(HttpPut.METHOD_NAME, "/index/_doc/4");
            doc4.setJsonEntity("{\"type\":\"type2\", \"id\":4, \"num\":100, \"num2\":10}");
            client().performRequest(doc4);
            Request doc5 = new Request(HttpPut.METHOD_NAME, "/index/_doc/5");
            doc5.setJsonEntity("{\"type\":\"type2\", \"id\":5, \"num\":100, \"num2\":10}");
            client().performRequest(doc5);
        }

        {
            Request doc1 = new Request(HttpPut.METHOD_NAME, "/index1/_doc/1");
            doc1.setJsonEntity("{\"id\":1, \"field\":\"value1\", \"rating\": 7}");
            client().performRequest(doc1);
            Request doc2 = new Request(HttpPut.METHOD_NAME, "/index1/_doc/2");
            doc2.setJsonEntity("{\"id\":2, \"field\":\"value2\"}");
            client().performRequest(doc2);
        }

        {
            Request create = new Request("PUT", "/index2");
            create.setJsonEntity(
                "{"
                    + "  \"mappings\": {"
                    + "    \"properties\": {"
                    + "      \"rating\": {"
                    + "        \"type\":  \"keyword\""
                    + "      }"
                    + "    }"
                    + "  }"
                    + "}"
            );
            client().performRequest(create);
            Request doc3 = new Request(HttpPut.METHOD_NAME, "/index2/_doc/3");
            doc3.setJsonEntity("{\"id\":3, \"field\":\"value1\", \"rating\": \"good\"}");
            client().performRequest(doc3);
            Request doc4 = new Request(HttpPut.METHOD_NAME, "/index2/_doc/4");
            doc4.setJsonEntity("{\"id\":4, \"field\":\"value2\"}");
            client().performRequest(doc4);
        }

        {
            Request doc5 = new Request(HttpPut.METHOD_NAME, "/index3/_doc/5");
            doc5.setJsonEntity("{\"id\":5, \"field\":\"value1\"}");
            client().performRequest(doc5);
            Request doc6 = new Request(HttpPut.METHOD_NAME, "/index3/_doc/6");
            doc6.setJsonEntity("{\"id\":6, \"field\":\"value2\"}");
            client().performRequest(doc6);
        }

        {
            Request create = new Request(HttpPut.METHOD_NAME, "/index4");
            create.setJsonEntity(
                "{"
                    + "  \"mappings\": {"
                    + "    \"properties\": {"
                    + "      \"field1\": {"
                    + "        \"type\":  \"keyword\","
                    + "        \"store\":  true"
                    + "      },"
                    + "      \"field2\": {"
                    + "        \"type\":  \"keyword\","
                    + "        \"store\":  true"
                    + "      }"
                    + "    }"
                    + "  }"
                    + "}"
            );
            client().performRequest(create);
            Request doc1 = new Request(HttpPut.METHOD_NAME, "/index4/_doc/1");
            doc1.setJsonEntity("{\"id\":1, \"field1\":\"value1\", \"field2\":\"value2\"}");
            client().performRequest(doc1);

            Request createFilteredAlias = new Request(HttpPost.METHOD_NAME, "/_aliases");
            createFilteredAlias.setJsonEntity(
                "{"
                    + "  \"actions\" : ["
                    + "    {"
                    + "      \"add\" : {"
                    + "        \"index\" : \"index4\","
                    + "        \"alias\" : \"alias4\","
                    + "        \"filter\" : { \"term\" : { \"field2\" : \"value1\" } }"
                    + "      }"
                    + "    }"
                    + "  ]"
                    + "}"
            );
            client().performRequest(createFilteredAlias);
        }

        client().performRequest(new Request(HttpPost.METHOD_NAME, "/_refresh"));
    }

    public void testSearchNoQuery() throws IOException {
        SearchRequest searchRequest = new SearchRequest("index");
        SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
        assertSearchHeader(searchResponse);
        assertNull(searchResponse.getAggregations());
        assertNull(searchResponse.getSuggest());
        assertEquals(Collections.emptyMap(), searchResponse.getProfileResults());
        assertEquals(5, searchResponse.getHits().getTotalHits().value);
        assertEquals(5, searchResponse.getHits().getHits().length);
        for (SearchHit searchHit : searchResponse.getHits().getHits()) {
            assertEquals("index", searchHit.getIndex());
            assertThat(Integer.valueOf(searchHit.getId()), both(greaterThan(0)).and(lessThan(6)));
            assertEquals(1.0f, searchHit.getScore(), 0);
            assertEquals(-1L, searchHit.getVersion());
            assertNotNull(searchHit.getSourceAsMap());
            assertEquals(4, searchHit.getSourceAsMap().size());
            assertTrue(searchHit.getSourceAsMap().containsKey("num"));
            assertTrue(searchHit.getSourceAsMap().containsKey("num2"));
        }
    }

    public void testSearchMatchQuery() throws IOException {
        SearchRequest searchRequest = new SearchRequest("index");
        searchRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("num", 10)));
        SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
        assertSearchHeader(searchResponse);
        assertNull(searchResponse.getAggregations());
        assertNull(searchResponse.getSuggest());
        assertEquals(Collections.emptyMap(), searchResponse.getProfileResults());
        assertEquals(1, searchResponse.getHits().getTotalHits().value);
        assertEquals(1, searchResponse.getHits().getHits().length);
        assertThat(searchResponse.getHits().getMaxScore(), greaterThan(0f));
        SearchHit searchHit = searchResponse.getHits().getHits()[0];
        assertEquals("index", searchHit.getIndex());
        assertEquals("1", searchHit.getId());
        assertThat(searchHit.getScore(), greaterThan(0f));
        assertEquals(-1L, searchHit.getVersion());
        assertNotNull(searchHit.getSourceAsMap());
        assertEquals(4, searchHit.getSourceAsMap().size());
        assertEquals("type1", searchHit.getSourceAsMap().get("type"));
        assertEquals(50, searchHit.getSourceAsMap().get("num2"));
    }

    public void testSearchWithTermsAgg() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.aggregation(new TermsAggregationBuilder("agg1").userValueTypeHint(ValueType.STRING).field("type.keyword"));
        searchSourceBuilder.size(0);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
        assertSearchHeader(searchResponse);
        assertNull(searchResponse.getSuggest());
        assertEquals(Collections.emptyMap(), searchResponse.getProfileResults());
        assertEquals(0, searchResponse.getHits().getHits().length);
        assertEquals(Float.NaN, searchResponse.getHits().getMaxScore(), 0f);
        Terms termsAgg = searchResponse.getAggregations().get("agg1");
        assertEquals("agg1", termsAgg.getName());
        assertEquals(2, termsAgg.getBuckets().size());
        Terms.Bucket type1 = termsAgg.getBucketByKey("type1");
        assertEquals(3, type1.getDocCount());
        assertEquals(0, type1.getAggregations().asList().size());
        Terms.Bucket type2 = termsAgg.getBucketByKey("type2");
        assertEquals(2, type2.getDocCount());
        assertEquals(0, type2.getAggregations().asList().size());
    }

    public void testSearchWithMultiTermsAgg() throws IOException {
        final String indexName = "multi_aggs";
        Request createIndex = new Request(HttpPut.METHOD_NAME, "/" + indexName);
        createIndex.setJsonEntity(
            "{\n"
                + "    \"mappings\": {\n"
                + "        \"properties\" : {\n"
                + "            \"username\" : {\n"
                + "                \"type\" : \"keyword\"\n"
                + "            },\n"
                + "            \"rating\" : {\n"
                + "                \"type\" : \"unsigned_long\"\n"
                + "            }\n"
                + "        }\n"
                + "    }"
                + "}"
        );
        client().performRequest(createIndex);

        {
            Request doc1 = new Request(HttpPut.METHOD_NAME, "/" + indexName + "/_doc/1");
            doc1.setJsonEntity("{\"username\":\"bob\", \"rating\": 18446744073709551615}");
            client().performRequest(doc1);
            Request doc2 = new Request(HttpPut.METHOD_NAME, "/" + indexName + "/_doc/2");
            doc2.setJsonEntity("{\"username\":\"tom\", \"rating\": 10223372036854775807}");
            client().performRequest(doc2);
            Request doc3 = new Request(HttpPut.METHOD_NAME, "/" + indexName + "/_doc/3");
            doc3.setJsonEntity("{\"username\":\"john\"}");
            client().performRequest(doc3);
        }
        client().performRequest(new Request(HttpPost.METHOD_NAME, "/_refresh"));

        SearchRequest searchRequest = new SearchRequest().indices(indexName);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.aggregation(
            new MultiTermsAggregationBuilder("agg1").terms(
                Arrays.asList(
                    new MultiTermsValuesSourceConfig.Builder().setFieldName("username").build(),
                    new MultiTermsValuesSourceConfig.Builder().setFieldName("rating").build()
                )
            )
        );
        searchSourceBuilder.size(0);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
        assertSearchHeader(searchResponse);
        assertNull(searchResponse.getSuggest());
        assertEquals(Collections.emptyMap(), searchResponse.getProfileResults());
        assertEquals(0, searchResponse.getHits().getHits().length);
        assertEquals(Float.NaN, searchResponse.getHits().getMaxScore(), 0f);
        Terms termsAgg = searchResponse.getAggregations().get("agg1");
        assertEquals("agg1", termsAgg.getName());
        assertEquals(2, termsAgg.getBuckets().size());
        Terms.Bucket bucket1 = termsAgg.getBucketByKey("bob|18446744073709551615");
        assertEquals(1, bucket1.getDocCount());
        assertEquals(0, bucket1.getAggregations().asList().size());
        Terms.Bucket bucket2 = termsAgg.getBucketByKey("tom|10223372036854775807");
        assertEquals(1, bucket2.getDocCount());
        assertEquals(0, bucket2.getAggregations().asList().size());
    }

    public void testSearchWithRareTermsAgg() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.aggregation(
            new RareTermsAggregationBuilder("agg1").userValueTypeHint(ValueType.STRING).field("type.keyword").maxDocCount(2)
        );
        searchSourceBuilder.size(0);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
        assertSearchHeader(searchResponse);
        assertNull(searchResponse.getSuggest());
        assertEquals(Collections.emptyMap(), searchResponse.getProfileResults());
        assertEquals(0, searchResponse.getHits().getHits().length);
        RareTerms termsAgg = searchResponse.getAggregations().get("agg1");
        assertEquals("agg1", termsAgg.getName());
        assertEquals(1, termsAgg.getBuckets().size());
        RareTerms.Bucket type2 = termsAgg.getBucketByKey("type2");
        assertEquals(2, type2.getDocCount());
        assertEquals(0, type2.getAggregations().asList().size());
    }

    public void testSearchWithCompositeAgg() throws IOException {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        List<CompositeValuesSourceBuilder<?>> sources = Collections.singletonList(
            new TermsValuesSourceBuilder("terms").field("type.keyword").missingBucket(true).order("asc")
        );
        searchSourceBuilder.aggregation(AggregationBuilders.composite("composite", sources));
        searchSourceBuilder.size(0);
        searchRequest.source(searchSourceBuilder);
        searchRequest.indices("index");
        SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
        assertSearchHeader(searchResponse);
        assertNull(searchResponse.getSuggest());
        assertEquals(Collections.emptyMap(), searchResponse.getProfileResults());
        assertEquals(0, searchResponse.getHits().getHits().length);
        assertEquals(Float.NaN, searchResponse.getHits().getMaxScore(), 0f);
        CompositeAggregation compositeAgg = searchResponse.getAggregations().get("composite");
        assertEquals("composite", compositeAgg.getName());
        assertEquals(2, compositeAgg.getBuckets().size());
        CompositeAggregation.Bucket bucket1 = compositeAgg.getBuckets().get(0);
        assertEquals(3, bucket1.getDocCount());
        assertEquals("{terms=type1}", bucket1.getKeyAsString());
        assertEquals(0, bucket1.getAggregations().asList().size());
        CompositeAggregation.Bucket bucket2 = compositeAgg.getBuckets().get(1);
        assertEquals(2, bucket2.getDocCount());
        assertEquals("{terms=type2}", bucket2.getKeyAsString());
        assertEquals(0, bucket2.getAggregations().asList().size());
    }

    public void testSearchWithRangeAgg() throws IOException {
        {
            SearchRequest searchRequest = new SearchRequest();
            SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
            searchSourceBuilder.aggregation(new RangeAggregationBuilder("agg1").field("num"));
            searchSourceBuilder.size(0);
            searchRequest.source(searchSourceBuilder);

            OpenSearchStatusException exception = expectThrows(
                OpenSearchStatusException.class,
                () -> execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync)
            );
            assertEquals(RestStatus.BAD_REQUEST, exception.status());
        }

        SearchRequest searchRequest = new SearchRequest("index");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.aggregation(
            new RangeAggregationBuilder("agg1").field("num").addRange("first", 0, 30).addRange("second", 31, 200)
        );
        searchSourceBuilder.size(0);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
        assertSearchHeader(searchResponse);
        assertNull(searchResponse.getSuggest());
        assertEquals(Collections.emptyMap(), searchResponse.getProfileResults());
        assertEquals(5, searchResponse.getHits().getTotalHits().value);
        assertEquals(0, searchResponse.getHits().getHits().length);
        assertEquals(Float.NaN, searchResponse.getHits().getMaxScore(), 0f);
        Range rangeAgg = searchResponse.getAggregations().get("agg1");
        assertEquals("agg1", rangeAgg.getName());
        assertEquals(2, rangeAgg.getBuckets().size());
        {
            Range.Bucket bucket = rangeAgg.getBuckets().get(0);
            assertEquals("first", bucket.getKeyAsString());
            assertEquals(2, bucket.getDocCount());
        }
        {
            Range.Bucket bucket = rangeAgg.getBuckets().get(1);
            assertEquals("second", bucket.getKeyAsString());
            assertEquals(3, bucket.getDocCount());
        }
    }

    public void testSearchWithTermsAndRangeAgg() throws IOException {
        SearchRequest searchRequest = new SearchRequest("index");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder agg = new TermsAggregationBuilder("agg1").userValueTypeHint(ValueType.STRING).field("type.keyword");
        agg.subAggregation(new RangeAggregationBuilder("subagg").field("num").addRange("first", 0, 30).addRange("second", 31, 200));
        searchSourceBuilder.aggregation(agg);
        searchSourceBuilder.size(0);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
        assertSearchHeader(searchResponse);
        assertNull(searchResponse.getSuggest());
        assertEquals(Collections.emptyMap(), searchResponse.getProfileResults());
        assertEquals(0, searchResponse.getHits().getHits().length);
        assertEquals(Float.NaN, searchResponse.getHits().getMaxScore(), 0f);
        Terms termsAgg = searchResponse.getAggregations().get("agg1");
        assertEquals("agg1", termsAgg.getName());
        assertEquals(2, termsAgg.getBuckets().size());
        Terms.Bucket type1 = termsAgg.getBucketByKey("type1");
        assertEquals(3, type1.getDocCount());
        assertEquals(1, type1.getAggregations().asList().size());
        {
            Range rangeAgg = type1.getAggregations().get("subagg");
            assertEquals(2, rangeAgg.getBuckets().size());
            {
                Range.Bucket bucket = rangeAgg.getBuckets().get(0);
                assertEquals("first", bucket.getKeyAsString());
                assertEquals(2, bucket.getDocCount());
            }
            {
                Range.Bucket bucket = rangeAgg.getBuckets().get(1);
                assertEquals("second", bucket.getKeyAsString());
                assertEquals(1, bucket.getDocCount());
            }
        }
        Terms.Bucket type2 = termsAgg.getBucketByKey("type2");
        assertEquals(2, type2.getDocCount());
        assertEquals(1, type2.getAggregations().asList().size());
        {
            Range rangeAgg = type2.getAggregations().get("subagg");
            assertEquals(2, rangeAgg.getBuckets().size());
            {
                Range.Bucket bucket = rangeAgg.getBuckets().get(0);
                assertEquals("first", bucket.getKeyAsString());
                assertEquals(0, bucket.getDocCount());
            }
            {
                Range.Bucket bucket = rangeAgg.getBuckets().get(1);
                assertEquals("second", bucket.getKeyAsString());
                assertEquals(2, bucket.getDocCount());
            }
        }
    }

    public void testSearchWithTermsAndWeightedAvg() throws IOException {
        SearchRequest searchRequest = new SearchRequest("index");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        TermsAggregationBuilder agg = new TermsAggregationBuilder("agg1").userValueTypeHint(ValueType.STRING).field("type.keyword");
        agg.subAggregation(
            new WeightedAvgAggregationBuilder("subagg").value(new MultiValuesSourceFieldConfig.Builder().setFieldName("num").build())
                .weight(new MultiValuesSourceFieldConfig.Builder().setFieldName("num2").build())
        );
        searchSourceBuilder.aggregation(agg);
        searchSourceBuilder.size(0);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
        assertSearchHeader(searchResponse);
        assertNull(searchResponse.getSuggest());
        assertEquals(Collections.emptyMap(), searchResponse.getProfileResults());
        assertEquals(0, searchResponse.getHits().getHits().length);
        assertEquals(Float.NaN, searchResponse.getHits().getMaxScore(), 0f);
        Terms termsAgg = searchResponse.getAggregations().get("agg1");
        assertEquals("agg1", termsAgg.getName());
        assertEquals(2, termsAgg.getBuckets().size());
        Terms.Bucket type1 = termsAgg.getBucketByKey("type1");
        assertEquals(3, type1.getDocCount());
        assertEquals(1, type1.getAggregations().asList().size());
        {
            WeightedAvg weightedAvg = type1.getAggregations().get("subagg");
            assertEquals(24.4, weightedAvg.getValue(), 0f);
        }
        Terms.Bucket type2 = termsAgg.getBucketByKey("type2");
        assertEquals(2, type2.getDocCount());
        assertEquals(1, type2.getAggregations().asList().size());
        {
            WeightedAvg weightedAvg = type2.getAggregations().get("subagg");
            assertEquals(100, weightedAvg.getValue(), 0f);
        }
    }

    public void testSearchWithMatrixStats() throws IOException {
        SearchRequest searchRequest = new SearchRequest("index");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.aggregation(new MatrixStatsAggregationBuilder("agg1").fields(Arrays.asList("num", "num2")));
        searchSourceBuilder.size(0);
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
        assertSearchHeader(searchResponse);
        assertNull(searchResponse.getSuggest());
        assertEquals(Collections.emptyMap(), searchResponse.getProfileResults());
        assertEquals(5, searchResponse.getHits().getTotalHits().value);
        assertEquals(0, searchResponse.getHits().getHits().length);
        assertEquals(Float.NaN, searchResponse.getHits().getMaxScore(), 0f);
        assertEquals(1, searchResponse.getAggregations().asList().size());
        MatrixStats matrixStats = searchResponse.getAggregations().get("agg1");
        assertEquals(5, matrixStats.getFieldCount("num"));
        assertEquals(56d, matrixStats.getMean("num"), 0d);
        assertEquals(1830.0000000000002, matrixStats.getVariance("num"), 0d);
        assertEquals(0.09340198804973039, matrixStats.getSkewness("num"), 0d);
        assertEquals(1.2741646510794589, matrixStats.getKurtosis("num"), 0d);
        assertEquals(5, matrixStats.getFieldCount("num2"));
        assertEquals(29d, matrixStats.getMean("num2"), 0d);
        assertEquals(330d, matrixStats.getVariance("num2"), 0d);
        assertEquals(-0.13568039346585542, matrixStats.getSkewness("num2"), 1.0e-16);
        assertEquals(1.3517561983471071, matrixStats.getKurtosis("num2"), 0d);
        assertEquals(-767.5, matrixStats.getCovariance("num", "num2"), 0d);
        assertEquals(-0.9876336291667923, matrixStats.getCorrelation("num", "num2"), 0d);
    }

    public void testSearchWithParentJoin() throws IOException {
        final String indexName = "child_example";
        Request createIndex = new Request(HttpPut.METHOD_NAME, "/" + indexName);
        createIndex.setJsonEntity(
            "{\n"
                + "    \"mappings\": {\n"
                + "        \"properties\" : {\n"
                + "            \"qa_join_field\" : {\n"
                + "                \"type\" : \"join\",\n"
                + "                \"relations\" : { \"question\" : \"answer\" }\n"
                + "            }\n"
                + "        }\n"
                + "    }"
                + "}"
        );
        client().performRequest(createIndex);
        Request questionDoc = new Request(HttpPut.METHOD_NAME, "/" + indexName + "/_doc/1");
        questionDoc.setJsonEntity(
            "{\n"
                + "    \"body\": \"<p>I have Windows 2003 server and i bought a new Windows 2008 server...\",\n"
                + "    \"title\": \"Whats the best way to file transfer my site from server to a newer one?\",\n"
                + "    \"tags\": [\n"
                + "        \"windows-server-2003\",\n"
                + "        \"windows-server-2008\",\n"
                + "        \"file-transfer\"\n"
                + "    ],\n"
                + "    \"qa_join_field\" : \"question\"\n"
                + "}"
        );
        client().performRequest(questionDoc);
        Request answerDoc1 = new Request(HttpPut.METHOD_NAME, "/" + indexName + "/_doc/2");
        answerDoc1.addParameter("routing", "1");
        answerDoc1.setJsonEntity(
            "{\n"
                + "    \"owner\": {\n"
                + "        \"location\": \"Norfolk, United Kingdom\",\n"
                + "        \"display_name\": \"Sam\",\n"
                + "        \"id\": 48\n"
                + "    },\n"
                + "    \"body\": \"<p>Unfortunately you're pretty much limited to FTP...\",\n"
                + "    \"qa_join_field\" : {\n"
                + "        \"name\" : \"answer\",\n"
                + "        \"parent\" : \"1\"\n"
                + "    },\n"
                + "    \"creation_date\": \"2009-05-04T13:45:37.030\"\n"
                + "}"
        );
        client().performRequest(answerDoc1);
        Request answerDoc2 = new Request(HttpPut.METHOD_NAME, "/" + indexName + "/_doc/3");
        answerDoc2.addParameter("routing", "1");
        answerDoc2.setJsonEntity(
            "{\n"
                + "    \"owner\": {\n"
                + "        \"location\": \"Norfolk, United Kingdom\",\n"
                + "        \"display_name\": \"Troll\",\n"
                + "        \"id\": 49\n"
                + "    },\n"
                + "    \"body\": \"<p>Use Linux...\",\n"
                + "    \"qa_join_field\" : {\n"
                + "        \"name\" : \"answer\",\n"
                + "        \"parent\" : \"1\"\n"
                + "    },\n"
                + "    \"creation_date\": \"2009-05-05T13:45:37.030\"\n"
                + "}"
        );
        client().performRequest(answerDoc2);
        client().performRequest(new Request(HttpPost.METHOD_NAME, "/_refresh"));

        TermsAggregationBuilder leafTermAgg = new TermsAggregationBuilder("top-names").userValueTypeHint(ValueType.STRING)
            .field("owner.display_name.keyword")
            .size(10);
        ChildrenAggregationBuilder childrenAgg = new ChildrenAggregationBuilder("to-answers", "answer").subAggregation(leafTermAgg);
        TermsAggregationBuilder termsAgg = new TermsAggregationBuilder("top-tags").userValueTypeHint(ValueType.STRING)
            .field("tags.keyword")
            .size(10)
            .subAggregation(childrenAgg);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.size(0).aggregation(termsAgg);
        SearchRequest searchRequest = new SearchRequest(indexName);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
        assertSearchHeader(searchResponse);
        assertNull(searchResponse.getSuggest());
        assertEquals(Collections.emptyMap(), searchResponse.getProfileResults());
        assertEquals(3, searchResponse.getHits().getTotalHits().value);
        assertEquals(0, searchResponse.getHits().getHits().length);
        assertEquals(Float.NaN, searchResponse.getHits().getMaxScore(), 0f);
        assertEquals(1, searchResponse.getAggregations().asList().size());
        Terms terms = searchResponse.getAggregations().get("top-tags");
        assertEquals(0, terms.getDocCountError());
        assertEquals(0, terms.getSumOfOtherDocCounts());
        assertEquals(3, terms.getBuckets().size());
        for (Terms.Bucket bucket : terms.getBuckets()) {
            assertThat(
                bucket.getKeyAsString(),
                either(equalTo("file-transfer")).or(equalTo("windows-server-2003")).or(equalTo("windows-server-2008"))
            );
            assertEquals(1, bucket.getDocCount());
            assertEquals(1, bucket.getAggregations().asList().size());
            Children children = bucket.getAggregations().get("to-answers");
            assertEquals(2, children.getDocCount());
            assertEquals(1, children.getAggregations().asList().size());
            Terms leafTerms = children.getAggregations().get("top-names");
            assertEquals(0, leafTerms.getDocCountError());
            assertEquals(0, leafTerms.getSumOfOtherDocCounts());
            assertEquals(2, leafTerms.getBuckets().size());
            assertEquals(2, leafTerms.getBuckets().size());
            Terms.Bucket sam = leafTerms.getBucketByKey("Sam");
            assertEquals(1, sam.getDocCount());
            Terms.Bucket troll = leafTerms.getBucketByKey("Troll");
            assertEquals(1, troll.getDocCount());
        }
    }

    public void testSearchWithSuggest() throws IOException {
        SearchRequest searchRequest = new SearchRequest("index");
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.suggest(new SuggestBuilder().addSuggestion("sugg1", new PhraseSuggestionBuilder("type")).setGlobalText("type"));
        searchSourceBuilder.size(0);
        searchRequest.source(searchSourceBuilder);

        SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
        assertSearchHeader(searchResponse);
        assertNull(searchResponse.getAggregations());
        assertEquals(Collections.emptyMap(), searchResponse.getProfileResults());
        assertEquals(0, searchResponse.getHits().getTotalHits().value);
        assertEquals(Float.NaN, searchResponse.getHits().getMaxScore(), 0f);
        assertEquals(0, searchResponse.getHits().getHits().length);
        assertEquals(1, searchResponse.getSuggest().size());

        Suggest.Suggestion<? extends Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option>> sugg = searchResponse.getSuggest()
            .iterator()
            .next();
        assertEquals("sugg1", sugg.getName());
        for (Suggest.Suggestion.Entry<? extends Suggest.Suggestion.Entry.Option> options : sugg) {
            assertEquals("type", options.getText().string());
            assertEquals(0, options.getOffset());
            assertEquals(4, options.getLength());
            assertEquals(2, options.getOptions().size());
            for (Suggest.Suggestion.Entry.Option option : options) {
                assertThat(option.getScore(), greaterThan(0f));
                assertThat(option.getText().string(), either(equalTo("type1")).or(equalTo("type2")));
            }
        }
    }

    public void testSearchWithWeirdScriptFields() throws Exception {
        Request doc = new Request("PUT", "/test/_doc/1");
        doc.setJsonEntity("{\"field\":\"value\"}");
        client().performRequest(doc);
        client().performRequest(new Request("POST", "/test/_refresh"));

        {
            SearchRequest searchRequest = new SearchRequest("test").source(
                SearchSourceBuilder.searchSource().scriptField("result", new Script("null"))
            );
            SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
            SearchHit searchHit = searchResponse.getHits().getAt(0);
            List<Object> values = searchHit.getFields().get("result").getValues();
            assertNotNull(values);
            assertEquals(1, values.size());
            assertNull(values.get(0));
        }
        {
            SearchRequest searchRequest = new SearchRequest("test").source(
                SearchSourceBuilder.searchSource().scriptField("result", new Script("new HashMap()"))
            );
            SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
            SearchHit searchHit = searchResponse.getHits().getAt(0);
            List<Object> values = searchHit.getFields().get("result").getValues();
            assertNotNull(values);
            assertEquals(1, values.size());
            assertThat(values.get(0), instanceOf(Map.class));
            Map<?, ?> map = (Map<?, ?>) values.get(0);
            assertEquals(0, map.size());
        }
        {
            SearchRequest searchRequest = new SearchRequest("test").source(
                SearchSourceBuilder.searchSource().scriptField("result", new Script("new String[]{}"))
            );
            SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
            SearchHit searchHit = searchResponse.getHits().getAt(0);
            List<Object> values = searchHit.getFields().get("result").getValues();
            assertNotNull(values);
            assertEquals(1, values.size());
            assertThat(values.get(0), instanceOf(List.class));
            List<?> list = (List<?>) values.get(0);
            assertEquals(0, list.size());
        }
    }

    public void testSearchWithDerivedFields() throws Exception {
        // Just testing DerivedField definition from SearchSourceBuilder derivedField()
        // We are not testing the full functionality here
        Request doc = new Request("PUT", "/test/_doc/1");
        doc.setJsonEntity("{\"field\":\"value\"}");
        client().performRequest(doc);
        client().performRequest(new Request("POST", "/test/_refresh"));
        // Keyword field
        {
            SearchRequest searchRequest = new SearchRequest("test").source(
                SearchSourceBuilder.searchSource()
                    .derivedField("result", "keyword", new Script("emit(params._source[\"field\"])"))
                    .fetchField("result")
                    .query(new TermsQueryBuilder("result", "value"))
            );
            SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
            SearchHit searchHit = searchResponse.getHits().getAt(0);
            List<Object> values = searchHit.getFields().get("result").getValues();
            assertNotNull(values);
            assertEquals(1, values.size());
            assertEquals("value", values.get(0));

            // multi valued
            searchRequest = new SearchRequest("test").source(
                SearchSourceBuilder.searchSource()
                    .derivedField(
                        "result",
                        "keyword",
                        new Script("emit(params._source[\"field\"]);emit(params._source[\"field\"] + \"_2\")")
                    )
                    .query(new TermsQueryBuilder("result", "value_2"))
                    .fetchField("result")
            );
            searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
            searchHit = searchResponse.getHits().getAt(0);
            values = searchHit.getFields().get("result").getValues();
            assertNotNull(values);
            assertEquals(2, values.size());
            assertEquals("value", values.get(0));
            assertEquals("value_2", values.get(1));
        }
        // Boolean field
        {
            SearchRequest searchRequest = new SearchRequest("test").source(
                SearchSourceBuilder.searchSource()
                    .derivedField("result", "boolean", new Script("emit(((String)params._source[\"field\"]).equals(\"value\"))"))
                    .query(new TermsQueryBuilder("result", "true"))
                    .fetchField("result")
            );
            SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
            SearchHit searchHit = searchResponse.getHits().getAt(0);
            List<Object> values = searchHit.getFields().get("result").getValues();
            assertNotNull(values);
            assertEquals(1, values.size());
            assertEquals(true, values.get(0));
        }
        // Long field
        {
            SearchRequest searchRequest = new SearchRequest("test").source(
                SearchSourceBuilder.searchSource()
                    .derivedField("result", "long", new Script("emit(Long.MAX_VALUE)"))
                    .query(new RangeQueryBuilder("result").from(Long.MAX_VALUE - 1).to(Long.MAX_VALUE))
                    .fetchField("result")
            );

            SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
            SearchHit searchHit = searchResponse.getHits().getAt(0);
            List<Object> values = searchHit.getFields().get("result").getValues();
            assertNotNull(values);
            assertEquals(1, values.size());
            assertEquals(Long.MAX_VALUE, values.get(0));

            // multi-valued
            searchRequest = new SearchRequest("test").source(
                SearchSourceBuilder.searchSource()
                    .derivedField("result", "long", new Script("emit(Long.MAX_VALUE); emit(Long.MIN_VALUE);"))
                    .query(new RangeQueryBuilder("result").from(Long.MIN_VALUE).to(Long.MIN_VALUE + 1))
                    .fetchField("result")
            );

            searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
            searchHit = searchResponse.getHits().getAt(0);
            values = searchHit.getFields().get("result").getValues();
            assertNotNull(values);
            assertEquals(2, values.size());
            assertEquals(Long.MAX_VALUE, values.get(0));
            assertEquals(Long.MIN_VALUE, values.get(1));
        }
        // Double field
        {
            SearchRequest searchRequest = new SearchRequest("test").source(
                SearchSourceBuilder.searchSource()
                    .derivedField("result", "double", new Script("emit(Double.MAX_VALUE)"))
                    .query(new RangeQueryBuilder("result").from(Double.MAX_VALUE - 1).to(Double.MAX_VALUE))
                    .fetchField("result")
            );
            SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
            SearchHit searchHit = searchResponse.getHits().getAt(0);
            List<Object> values = searchHit.getFields().get("result").getValues();
            assertNotNull(values);
            assertEquals(1, values.size());
            assertEquals(Double.MAX_VALUE, values.get(0));

            // multi-valued
            searchRequest = new SearchRequest("test").source(
                SearchSourceBuilder.searchSource()
                    .derivedField("result", "double", new Script("emit(Double.MAX_VALUE); emit(Double.MIN_VALUE);"))
                    .query(new RangeQueryBuilder("result").from(Double.MIN_VALUE).to(Double.MIN_VALUE + 1))
                    .fetchField("result")
            );

            searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
            searchHit = searchResponse.getHits().getAt(0);
            values = searchHit.getFields().get("result").getValues();
            assertNotNull(values);
            assertEquals(2, values.size());
            assertEquals(Double.MAX_VALUE, values.get(0));
            assertEquals(Double.MIN_VALUE, values.get(1));
        }
        // Date field
        {
            DateTime date1 = new DateTime(1990, 12, 29, 0, 0, DateTimeZone.UTC);
            DateTime date2 = new DateTime(1990, 12, 30, 0, 0, DateTimeZone.UTC);
            SearchRequest searchRequest = new SearchRequest("test").source(
                SearchSourceBuilder.searchSource()
                    .derivedField("result", "date", new Script("emit(" + date1.getMillis() + "L)"))
                    .query(new RangeQueryBuilder("result").from(date1.toString()).to(date2.toString()))
                    .fetchField("result")
            );

            SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
            SearchHit searchHit = searchResponse.getHits().getAt(0);
            List<Object> values = searchHit.getFields().get("result").getValues();
            assertNotNull(values);
            assertEquals(1, values.size());
            assertEquals(date1.toString(), values.get(0));

            // multi-valued
            searchRequest = new SearchRequest("test").source(
                SearchSourceBuilder.searchSource()
                    .derivedField("result", "date", new Script("emit(" + date1.getMillis() + "L); " + "emit(" + date2.getMillis() + "L)"))
                    .query(new RangeQueryBuilder("result").from(date1.toString()).to(date2.toString()))
                    .fetchField("result")
            );

            searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
            searchHit = searchResponse.getHits().getAt(0);
            values = searchHit.getFields().get("result").getValues();
            assertNotNull(values);
            assertEquals(2, values.size());
            assertEquals(date1.toString(), values.get(0));
            assertEquals(date2.toString(), values.get(1));
        }
        // Geo field
        {
            GeoShapeQueryBuilder qb = geoShapeQuery("result", new Rectangle(-35, 35, 35, -35));
            qb.relation(ShapeRelation.INTERSECTS);
            SearchRequest searchRequest = new SearchRequest("test").source(
                SearchSourceBuilder.searchSource()
                    .derivedField("result", "geo_point", new Script("emit(10.0, 20.0)"))
                    .query(qb)
                    .fetchField("result")
            );

            SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
            SearchHit searchHit = searchResponse.getHits().getAt(0);
            List<Object> values = searchHit.getFields().get("result").getValues();
            assertNotNull(values);
            assertEquals(1, values.size());
            assertEquals(10.0, ((HashMap) values.get(0)).get("lat"));
            assertEquals(20.0, ((HashMap) values.get(0)).get("lon"));

            // multi-valued
            searchRequest = new SearchRequest("test").source(
                SearchSourceBuilder.searchSource()
                    .derivedField("result", "geo_point", new Script("emit(10.0, 20.0); emit(20.0, 30.0);"))
                    .query(qb)
                    .fetchField("result")
            );

            searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
            searchHit = searchResponse.getHits().getAt(0);
            values = searchHit.getFields().get("result").getValues();
            assertNotNull(values);
            assertEquals(2, values.size());
            assertEquals(10.0, ((HashMap) values.get(0)).get("lat"));
            assertEquals(20.0, ((HashMap) values.get(0)).get("lon"));
            assertEquals(20.0, ((HashMap) values.get(1)).get("lat"));
            assertEquals(30.0, ((HashMap) values.get(1)).get("lon"));
        }
        // IP field
        {
            SearchRequest searchRequest = new SearchRequest("test").source(
                SearchSourceBuilder.searchSource().derivedField("result", "ip", new Script("emit(\"10.0.0.1\")")).fetchField("result")
            );

            SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
            SearchHit searchHit = searchResponse.getHits().getAt(0);
            List<Object> values = searchHit.getFields().get("result").getValues();
            assertNotNull(values);
            assertEquals(1, values.size());
            assertEquals("10.0.0.1", values.get(0));

            // multi-valued
            searchRequest = new SearchRequest("test").source(
                SearchSourceBuilder.searchSource()
                    .derivedField("result", "ip", new Script("emit(\"10.0.0.1\"); emit(\"10.0.0.2\");"))
                    .fetchField("result")
            );

            searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);
            searchHit = searchResponse.getHits().getAt(0);
            values = searchHit.getFields().get("result").getValues();
            assertNotNull(values);
            assertEquals(2, values.size());
            assertEquals("10.0.0.1", values.get(0));
            assertEquals("10.0.0.2", values.get(1));

        }

    }

    public void testSearchScroll() throws Exception {
        for (int i = 0; i < 100; i++) {
            XContentBuilder builder = jsonBuilder().startObject().field("field", i).endObject();
            Request doc = new Request(HttpPut.METHOD_NAME, "/test/_doc/" + Integer.toString(i));
            doc.setJsonEntity(builder.toString());
            client().performRequest(doc);
        }
        client().performRequest(new Request(HttpPost.METHOD_NAME, "/test/_refresh"));

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().size(35).sort("field", SortOrder.ASC);
        SearchRequest searchRequest = new SearchRequest("test").scroll(TimeValue.timeValueMinutes(2)).source(searchSourceBuilder);
        SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);

        try {
            long counter = 0;
            assertSearchHeader(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(100L));
            assertThat(searchResponse.getHits().getHits().length, equalTo(35));
            for (SearchHit hit : searchResponse.getHits()) {
                assertThat(((Number) hit.getSortValues()[0]).longValue(), equalTo(counter++));
            }

            searchResponse = execute(
                new SearchScrollRequest(searchResponse.getScrollId()).scroll(TimeValue.timeValueMinutes(2)),
                highLevelClient()::scroll,
                highLevelClient()::scrollAsync
            );

            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(100L));
            assertThat(searchResponse.getHits().getHits().length, equalTo(35));
            for (SearchHit hit : searchResponse.getHits()) {
                assertEquals(counter++, ((Number) hit.getSortValues()[0]).longValue());
            }

            searchResponse = execute(
                new SearchScrollRequest(searchResponse.getScrollId()).scroll(TimeValue.timeValueMinutes(2)),
                highLevelClient()::scroll,
                highLevelClient()::scrollAsync
            );

            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(100L));
            assertThat(searchResponse.getHits().getHits().length, equalTo(30));
            for (SearchHit hit : searchResponse.getHits()) {
                assertEquals(counter++, ((Number) hit.getSortValues()[0]).longValue());
            }
        } finally {
            ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
            clearScrollRequest.addScrollId(searchResponse.getScrollId());
            ClearScrollResponse clearScrollResponse = execute(
                clearScrollRequest,
                highLevelClient()::clearScroll,
                highLevelClient()::clearScrollAsync
            );
            assertThat(clearScrollResponse.getNumFreed(), greaterThan(0));
            assertTrue(clearScrollResponse.isSucceeded());

            SearchScrollRequest scrollRequest = new SearchScrollRequest(searchResponse.getScrollId()).scroll(TimeValue.timeValueMinutes(2));
            OpenSearchStatusException exception = expectThrows(
                OpenSearchStatusException.class,
                () -> execute(scrollRequest, highLevelClient()::scroll, highLevelClient()::scrollAsync)
            );
            assertEquals(RestStatus.NOT_FOUND, exception.status());
            assertThat(exception.getRootCause(), instanceOf(OpenSearchException.class));
            OpenSearchException rootCause = (OpenSearchException) exception.getRootCause();
            assertThat(rootCause.getMessage(), containsString("No search context found for"));
        }
    }

    public void testSearchWithPit() throws Exception {
        for (int i = 0; i < 100; i++) {
            XContentBuilder builder = jsonBuilder().startObject().field("field", i).endObject();
            Request doc = new Request(HttpPut.METHOD_NAME, "/test/_doc/" + Integer.toString(i));
            doc.setJsonEntity(builder.toString());
            client().performRequest(doc);
        }
        client().performRequest(new Request(HttpPost.METHOD_NAME, "/test/_refresh"));

        CreatePitRequest pitRequest = new CreatePitRequest(new TimeValue(1, TimeUnit.DAYS), true, "test");
        CreatePitResponse pitResponse = execute(pitRequest, highLevelClient()::createPit, highLevelClient()::createPitAsync);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder().size(35)
            .sort("field", SortOrder.ASC)
            .pointInTimeBuilder(new PointInTimeBuilder(pitResponse.getId()));
        SearchRequest searchRequest = new SearchRequest().source(searchSourceBuilder);
        SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);

        try {
            long counter = 0;
            assertSearchHeader(searchResponse);
            assertThat(searchResponse.getHits().getTotalHits().value, equalTo(100L));
            assertThat(searchResponse.getHits().getHits().length, equalTo(35));
            for (SearchHit hit : searchResponse.getHits()) {
                assertThat(((Number) hit.getSortValues()[0]).longValue(), equalTo(counter++));
            }
        } finally {
            List<String> pitIds = new ArrayList<>();
            pitIds.add(pitResponse.getId());
            DeletePitRequest deletePitRequest = new DeletePitRequest(pitIds);
            DeletePitResponse deletePitResponse = execute(
                deletePitRequest,
                highLevelClient()::deletePit,
                highLevelClient()::deletePitAsync
            );
            assertTrue(deletePitResponse.getDeletePitResults().get(0).isSuccessful());
            assertTrue(deletePitResponse.getDeletePitResults().get(0).getPitId().equals(pitResponse.getId()));
        }
    }

    public void testMultiSearch() throws Exception {
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        SearchRequest searchRequest1 = new SearchRequest("index1");
        searchRequest1.source().sort("id", SortOrder.ASC);
        multiSearchRequest.add(searchRequest1);
        SearchRequest searchRequest2 = new SearchRequest("index2");
        searchRequest2.source().sort("id", SortOrder.ASC);
        multiSearchRequest.add(searchRequest2);
        SearchRequest searchRequest3 = new SearchRequest("index3");
        searchRequest3.source().sort("id", SortOrder.ASC);
        multiSearchRequest.add(searchRequest3);

        MultiSearchResponse multiSearchResponse = execute(multiSearchRequest, highLevelClient()::msearch, highLevelClient()::msearchAsync);
        assertThat(multiSearchResponse.getTook().millis(), Matchers.greaterThanOrEqualTo(0L));
        assertThat(multiSearchResponse.getResponses().length, Matchers.equalTo(3));

        assertThat(multiSearchResponse.getResponses()[0].getFailure(), Matchers.nullValue());
        assertThat(multiSearchResponse.getResponses()[0].isFailure(), Matchers.is(false));
        SearchIT.assertSearchHeader(multiSearchResponse.getResponses()[0].getResponse());
        assertThat(multiSearchResponse.getResponses()[0].getResponse().getHits().getTotalHits().value, Matchers.equalTo(2L));
        assertThat(multiSearchResponse.getResponses()[0].getResponse().getHits().getAt(0).getId(), Matchers.equalTo("1"));
        assertThat(multiSearchResponse.getResponses()[0].getResponse().getHits().getAt(1).getId(), Matchers.equalTo("2"));

        assertThat(multiSearchResponse.getResponses()[1].getFailure(), Matchers.nullValue());
        assertThat(multiSearchResponse.getResponses()[1].isFailure(), Matchers.is(false));
        SearchIT.assertSearchHeader(multiSearchResponse.getResponses()[1].getResponse());
        assertThat(multiSearchResponse.getResponses()[1].getResponse().getHits().getTotalHits().value, Matchers.equalTo(2L));
        assertThat(multiSearchResponse.getResponses()[1].getResponse().getHits().getAt(0).getId(), Matchers.equalTo("3"));
        assertThat(multiSearchResponse.getResponses()[1].getResponse().getHits().getAt(1).getId(), Matchers.equalTo("4"));

        assertThat(multiSearchResponse.getResponses()[2].getFailure(), Matchers.nullValue());
        assertThat(multiSearchResponse.getResponses()[2].isFailure(), Matchers.is(false));
        SearchIT.assertSearchHeader(multiSearchResponse.getResponses()[2].getResponse());
        assertThat(multiSearchResponse.getResponses()[2].getResponse().getHits().getTotalHits().value, Matchers.equalTo(2L));
        assertThat(multiSearchResponse.getResponses()[2].getResponse().getHits().getAt(0).getId(), Matchers.equalTo("5"));
        assertThat(multiSearchResponse.getResponses()[2].getResponse().getHits().getAt(1).getId(), Matchers.equalTo("6"));
    }

    public void testSearchWithSort() throws Exception {
        final String indexName = "search_sort";
        Request createIndex = new Request(HttpPut.METHOD_NAME, "/" + indexName);
        createIndex.setJsonEntity(
            "{\n"
                + "    \"mappings\": {\n"
                + "        \"properties\" : {\n"
                + "            \"username\" : {\n"
                + "                \"type\" : \"keyword\"\n"
                + "            },\n"
                + "            \"rating\" : {\n"
                + "                \"type\" : \"unsigned_long\"\n"
                + "            }\n"
                + "        }\n"
                + "    }"
                + "}"
        );
        client().performRequest(createIndex);

        {
            Request doc1 = new Request(HttpPut.METHOD_NAME, "/" + indexName + "/_doc/1");
            doc1.setJsonEntity("{\"username\":\"bob\", \"rating\": 18446744073709551610}");
            client().performRequest(doc1);
            Request doc2 = new Request(HttpPut.METHOD_NAME, "/" + indexName + "/_doc/2");
            doc2.setJsonEntity("{\"username\":\"tom\", \"rating\": 10223372036854775807}");
            client().performRequest(doc2);
            Request doc3 = new Request(HttpPut.METHOD_NAME, "/" + indexName + "/_doc/3");
            doc3.setJsonEntity("{\"username\":\"john\"}");
            client().performRequest(doc3);
        }
        client().performRequest(new Request(HttpPost.METHOD_NAME, "/search_sort/_refresh"));

        SearchRequest searchRequest = new SearchRequest("search_sort");
        searchRequest.source().sort("rating", SortOrder.ASC);
        SearchResponse searchResponse = execute(searchRequest, highLevelClient()::search, highLevelClient()::searchAsync);

        assertThat(searchResponse.getTook().millis(), Matchers.greaterThanOrEqualTo(0L));
        assertThat(searchResponse.getHits().getTotalHits().value, Matchers.equalTo(3L));
        assertThat(searchResponse.getHits().getAt(0).getId(), Matchers.equalTo("2"));
        assertThat(searchResponse.getHits().getAt(1).getId(), Matchers.equalTo("1"));
        assertThat(searchResponse.getHits().getAt(2).getId(), Matchers.equalTo("3"));

        assertThat(searchResponse.getHits().getAt(0).getSortValues().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(0).getSortValues()[0], equalTo(new BigInteger("10223372036854775807")));
        assertThat(searchResponse.getHits().getAt(1).getSortValues().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(1).getSortValues()[0], equalTo(new BigInteger("18446744073709551610")));
        assertThat(searchResponse.getHits().getAt(2).getSortValues().length, equalTo(1));
        assertThat(searchResponse.getHits().getAt(2).getSortValues()[0], equalTo(new BigInteger("18446744073709551615")));
    }

    public void testMultiSearch_withAgg() throws Exception {
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        SearchRequest searchRequest1 = new SearchRequest("index1");
        searchRequest1.source()
            .size(0)
            .aggregation(
                new TermsAggregationBuilder("name").userValueTypeHint(ValueType.STRING).field("field.keyword").order(BucketOrder.key(true))
            );
        multiSearchRequest.add(searchRequest1);
        SearchRequest searchRequest2 = new SearchRequest("index2");
        searchRequest2.source()
            .size(0)
            .aggregation(
                new TermsAggregationBuilder("name").userValueTypeHint(ValueType.STRING).field("field.keyword").order(BucketOrder.key(true))
            );
        multiSearchRequest.add(searchRequest2);
        SearchRequest searchRequest3 = new SearchRequest("index3");
        searchRequest3.source()
            .size(0)
            .aggregation(
                new TermsAggregationBuilder("name").userValueTypeHint(ValueType.STRING).field("field.keyword").order(BucketOrder.key(true))
            );
        multiSearchRequest.add(searchRequest3);

        MultiSearchResponse multiSearchResponse = execute(multiSearchRequest, highLevelClient()::msearch, highLevelClient()::msearchAsync);
        assertThat(multiSearchResponse.getTook().millis(), Matchers.greaterThanOrEqualTo(0L));
        assertThat(multiSearchResponse.getResponses().length, Matchers.equalTo(3));

        assertThat(multiSearchResponse.getResponses()[0].getFailure(), Matchers.nullValue());
        assertThat(multiSearchResponse.getResponses()[0].isFailure(), Matchers.is(false));
        SearchIT.assertSearchHeader(multiSearchResponse.getResponses()[0].getResponse());
        assertThat(multiSearchResponse.getResponses()[0].getResponse().getHits().getTotalHits().value, Matchers.equalTo(2L));
        assertThat(multiSearchResponse.getResponses()[0].getResponse().getHits().getHits().length, Matchers.equalTo(0));
        Terms terms = multiSearchResponse.getResponses()[0].getResponse().getAggregations().get("name");
        assertThat(terms.getBuckets().size(), Matchers.equalTo(2));
        assertThat(terms.getBuckets().get(0).getKeyAsString(), Matchers.equalTo("value1"));
        assertThat(terms.getBuckets().get(1).getKeyAsString(), Matchers.equalTo("value2"));

        assertThat(multiSearchResponse.getResponses()[1].getFailure(), Matchers.nullValue());
        assertThat(multiSearchResponse.getResponses()[1].isFailure(), Matchers.is(false));
        SearchIT.assertSearchHeader(multiSearchResponse.getResponses()[0].getResponse());
        assertThat(multiSearchResponse.getResponses()[1].getResponse().getHits().getTotalHits().value, Matchers.equalTo(2L));
        assertThat(multiSearchResponse.getResponses()[1].getResponse().getHits().getHits().length, Matchers.equalTo(0));
        terms = multiSearchResponse.getResponses()[1].getResponse().getAggregations().get("name");
        assertThat(terms.getBuckets().size(), Matchers.equalTo(2));
        assertThat(terms.getBuckets().get(0).getKeyAsString(), Matchers.equalTo("value1"));
        assertThat(terms.getBuckets().get(1).getKeyAsString(), Matchers.equalTo("value2"));

        assertThat(multiSearchResponse.getResponses()[2].getFailure(), Matchers.nullValue());
        assertThat(multiSearchResponse.getResponses()[2].isFailure(), Matchers.is(false));
        SearchIT.assertSearchHeader(multiSearchResponse.getResponses()[0].getResponse());
        assertThat(multiSearchResponse.getResponses()[2].getResponse().getHits().getTotalHits().value, Matchers.equalTo(2L));
        assertThat(multiSearchResponse.getResponses()[2].getResponse().getHits().getHits().length, Matchers.equalTo(0));
        terms = multiSearchResponse.getResponses()[2].getResponse().getAggregations().get("name");
        assertThat(terms.getBuckets().size(), Matchers.equalTo(2));
        assertThat(terms.getBuckets().get(0).getKeyAsString(), Matchers.equalTo("value1"));
        assertThat(terms.getBuckets().get(1).getKeyAsString(), Matchers.equalTo("value2"));
    }

    public void testMultiSearch_withQuery() throws Exception {
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        SearchRequest searchRequest1 = new SearchRequest("index1");
        searchRequest1.source().query(new TermsQueryBuilder("field", "value2"));
        multiSearchRequest.add(searchRequest1);
        SearchRequest searchRequest2 = new SearchRequest("index2");
        searchRequest2.source().query(new TermsQueryBuilder("field", "value2"));
        multiSearchRequest.add(searchRequest2);
        SearchRequest searchRequest3 = new SearchRequest("index3");
        searchRequest3.source().query(new TermsQueryBuilder("field", "value2"));
        multiSearchRequest.add(searchRequest3);

        MultiSearchResponse multiSearchResponse = execute(multiSearchRequest, highLevelClient()::msearch, highLevelClient()::msearchAsync);
        assertThat(multiSearchResponse.getTook().millis(), Matchers.greaterThanOrEqualTo(0L));
        assertThat(multiSearchResponse.getResponses().length, Matchers.equalTo(3));

        assertThat(multiSearchResponse.getResponses()[0].getFailure(), Matchers.nullValue());
        assertThat(multiSearchResponse.getResponses()[0].isFailure(), Matchers.is(false));
        SearchIT.assertSearchHeader(multiSearchResponse.getResponses()[0].getResponse());
        assertThat(multiSearchResponse.getResponses()[0].getResponse().getHits().getTotalHits().value, Matchers.equalTo(1L));
        assertThat(multiSearchResponse.getResponses()[0].getResponse().getHits().getAt(0).getId(), Matchers.equalTo("2"));

        assertThat(multiSearchResponse.getResponses()[1].getFailure(), Matchers.nullValue());
        assertThat(multiSearchResponse.getResponses()[1].isFailure(), Matchers.is(false));
        SearchIT.assertSearchHeader(multiSearchResponse.getResponses()[1].getResponse());
        assertThat(multiSearchResponse.getResponses()[1].getResponse().getHits().getTotalHits().value, Matchers.equalTo(1L));
        assertThat(multiSearchResponse.getResponses()[1].getResponse().getHits().getAt(0).getId(), Matchers.equalTo("4"));

        assertThat(multiSearchResponse.getResponses()[2].getFailure(), Matchers.nullValue());
        assertThat(multiSearchResponse.getResponses()[2].isFailure(), Matchers.is(false));
        SearchIT.assertSearchHeader(multiSearchResponse.getResponses()[2].getResponse());
        assertThat(multiSearchResponse.getResponses()[2].getResponse().getHits().getTotalHits().value, Matchers.equalTo(1L));
        assertThat(multiSearchResponse.getResponses()[2].getResponse().getHits().getAt(0).getId(), Matchers.equalTo("6"));

        searchRequest1.source().highlighter(new HighlightBuilder().field("field"));
        searchRequest2.source().highlighter(new HighlightBuilder().field("field"));
        searchRequest3.source().highlighter(new HighlightBuilder().field("field"));
        multiSearchResponse = execute(multiSearchRequest, highLevelClient()::msearch, highLevelClient()::msearchAsync);
        assertThat(multiSearchResponse.getTook().millis(), Matchers.greaterThanOrEqualTo(0L));
        assertThat(multiSearchResponse.getResponses().length, Matchers.equalTo(3));

        assertThat(multiSearchResponse.getResponses()[0].getFailure(), Matchers.nullValue());
        assertThat(multiSearchResponse.getResponses()[0].isFailure(), Matchers.is(false));
        SearchIT.assertSearchHeader(multiSearchResponse.getResponses()[0].getResponse());
        assertThat(multiSearchResponse.getResponses()[0].getResponse().getHits().getTotalHits().value, Matchers.equalTo(1L));
        assertThat(
            multiSearchResponse.getResponses()[0].getResponse().getHits().getAt(0).getHighlightFields().get("field").fragments()[0]
                .string(),
            Matchers.equalTo("<em>value2</em>")
        );

        assertThat(multiSearchResponse.getResponses()[1].getFailure(), Matchers.nullValue());
        assertThat(multiSearchResponse.getResponses()[1].isFailure(), Matchers.is(false));
        SearchIT.assertSearchHeader(multiSearchResponse.getResponses()[1].getResponse());
        assertThat(multiSearchResponse.getResponses()[1].getResponse().getHits().getTotalHits().value, Matchers.equalTo(1L));
        assertThat(multiSearchResponse.getResponses()[1].getResponse().getHits().getAt(0).getId(), Matchers.equalTo("4"));
        assertThat(
            multiSearchResponse.getResponses()[1].getResponse().getHits().getAt(0).getHighlightFields().get("field").fragments()[0]
                .string(),
            Matchers.equalTo("<em>value2</em>")
        );

        assertThat(multiSearchResponse.getResponses()[2].getFailure(), Matchers.nullValue());
        assertThat(multiSearchResponse.getResponses()[2].isFailure(), Matchers.is(false));
        SearchIT.assertSearchHeader(multiSearchResponse.getResponses()[2].getResponse());
        assertThat(multiSearchResponse.getResponses()[2].getResponse().getHits().getTotalHits().value, Matchers.equalTo(1L));
        assertThat(multiSearchResponse.getResponses()[2].getResponse().getHits().getAt(0).getId(), Matchers.equalTo("6"));
        assertThat(
            multiSearchResponse.getResponses()[2].getResponse().getHits().getAt(0).getHighlightFields().get("field").fragments()[0]
                .string(),
            Matchers.equalTo("<em>value2</em>")
        );
    }

    public void testMultiSearch_failure() throws Exception {
        MultiSearchRequest multiSearchRequest = new MultiSearchRequest();
        SearchRequest searchRequest1 = new SearchRequest("index1");
        searchRequest1.source().query(new ScriptQueryBuilder(new Script(ScriptType.INLINE, "invalid", "code", Collections.emptyMap())));
        multiSearchRequest.add(searchRequest1);
        SearchRequest searchRequest2 = new SearchRequest("index2");
        searchRequest2.source().query(new ScriptQueryBuilder(new Script(ScriptType.INLINE, "invalid", "code", Collections.emptyMap())));
        multiSearchRequest.add(searchRequest2);

        MultiSearchResponse multiSearchResponse = execute(multiSearchRequest, highLevelClient()::msearch, highLevelClient()::msearchAsync);
        assertThat(multiSearchResponse.getTook().millis(), Matchers.greaterThanOrEqualTo(0L));
        assertThat(multiSearchResponse.getResponses().length, Matchers.equalTo(2));

        assertThat(multiSearchResponse.getResponses()[0].isFailure(), Matchers.is(true));
        assertThat(multiSearchResponse.getResponses()[0].getFailure().getMessage(), containsString("search_phase_execution_exception"));
        assertThat(multiSearchResponse.getResponses()[0].getResponse(), nullValue());

        assertThat(multiSearchResponse.getResponses()[1].isFailure(), Matchers.is(true));
        assertThat(multiSearchResponse.getResponses()[1].getFailure().getMessage(), containsString("search_phase_execution_exception"));
        assertThat(multiSearchResponse.getResponses()[1].getResponse(), nullValue());
    }

    public void testSearchTemplate() throws IOException {
        SearchTemplateRequest searchTemplateRequest = new SearchTemplateRequest();
        searchTemplateRequest.setRequest(new SearchRequest("index"));

        searchTemplateRequest.setScriptType(ScriptType.INLINE);
        searchTemplateRequest.setScript("{ \"query\": { \"match\": { \"num\": {{number}} } } }");

        Map<String, Object> scriptParams = new HashMap<>();
        scriptParams.put("number", 10);
        searchTemplateRequest.setScriptParams(scriptParams);

        searchTemplateRequest.setExplain(true);
        searchTemplateRequest.setProfile(true);

        SearchTemplateResponse searchTemplateResponse = execute(
            searchTemplateRequest,
            highLevelClient()::searchTemplate,
            highLevelClient()::searchTemplateAsync
        );

        assertNull(searchTemplateResponse.getSource());

        SearchResponse searchResponse = searchTemplateResponse.getResponse();
        assertNotNull(searchResponse);

        assertEquals(1, searchResponse.getHits().getTotalHits().value);
        assertEquals(1, searchResponse.getHits().getHits().length);
        assertThat(searchResponse.getHits().getMaxScore(), greaterThan(0f));

        SearchHit hit = searchResponse.getHits().getHits()[0];
        assertNotNull(hit.getExplanation());

        assertFalse(searchResponse.getProfileResults().isEmpty());
    }

    public void testNonExistentSearchTemplate() {
        SearchTemplateRequest searchTemplateRequest = new SearchTemplateRequest();
        searchTemplateRequest.setRequest(new SearchRequest("index"));

        searchTemplateRequest.setScriptType(ScriptType.STORED);
        searchTemplateRequest.setScript("non-existent");
        searchTemplateRequest.setScriptParams(Collections.emptyMap());

        OpenSearchStatusException exception = expectThrows(
            OpenSearchStatusException.class,
            () -> execute(searchTemplateRequest, highLevelClient()::searchTemplate, highLevelClient()::searchTemplateAsync)
        );

        assertEquals(RestStatus.NOT_FOUND, exception.status());
    }

    public void testRenderSearchTemplate() throws IOException {
        SearchTemplateRequest searchTemplateRequest = new SearchTemplateRequest();

        searchTemplateRequest.setScriptType(ScriptType.INLINE);
        searchTemplateRequest.setScript("{ \"query\": { \"match\": { \"num\": {{number}} } } }");

        Map<String, Object> scriptParams = new HashMap<>();
        scriptParams.put("number", 10);
        searchTemplateRequest.setScriptParams(scriptParams);

        // Setting simulate true causes the template to only be rendered.
        searchTemplateRequest.setSimulate(true);

        SearchTemplateResponse searchTemplateResponse = execute(
            searchTemplateRequest,
            highLevelClient()::searchTemplate,
            highLevelClient()::searchTemplateAsync
        );
        assertNull(searchTemplateResponse.getResponse());

        BytesReference expectedSource = BytesReference.bytes(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("query")
                .startObject("match")
                .field("num", 10)
                .endObject()
                .endObject()
                .endObject()
        );

        BytesReference actualSource = searchTemplateResponse.getSource();
        assertNotNull(actualSource);

        assertToXContentEquivalent(expectedSource, actualSource, MediaTypeRegistry.JSON);
    }

    public void testMultiSearchTemplate() throws Exception {
        MultiSearchTemplateRequest multiSearchTemplateRequest = new MultiSearchTemplateRequest();

        SearchTemplateRequest goodRequest = new SearchTemplateRequest();
        goodRequest.setRequest(new SearchRequest("index"));
        goodRequest.setScriptType(ScriptType.INLINE);
        goodRequest.setScript("{ \"query\": { \"match\": { \"num\": {{number}} } } }");
        Map<String, Object> scriptParams = new HashMap<>();
        scriptParams.put("number", 10);
        goodRequest.setScriptParams(scriptParams);
        goodRequest.setExplain(true);
        goodRequest.setProfile(true);
        multiSearchTemplateRequest.add(goodRequest);

        SearchTemplateRequest badRequest = new SearchTemplateRequest();
        badRequest.setRequest(new SearchRequest("index"));
        badRequest.setScriptType(ScriptType.INLINE);
        badRequest.setScript("{ NOT VALID JSON {{number}} }");
        scriptParams = new HashMap<>();
        scriptParams.put("number", 10);
        badRequest.setScriptParams(scriptParams);

        multiSearchTemplateRequest.add(badRequest);

        MultiSearchTemplateResponse multiSearchTemplateResponse = execute(
            multiSearchTemplateRequest,
            highLevelClient()::msearchTemplate,
            highLevelClient()::msearchTemplateAsync
        );

        Item[] responses = multiSearchTemplateResponse.getResponses();

        assertEquals(2, responses.length);

        assertNull(responses[0].getResponse().getSource());
        SearchResponse goodResponse = responses[0].getResponse().getResponse();
        assertNotNull(goodResponse);
        assertThat(responses[0].isFailure(), Matchers.is(false));
        assertEquals(1, goodResponse.getHits().getTotalHits().value);
        assertEquals(1, goodResponse.getHits().getHits().length);
        assertThat(goodResponse.getHits().getMaxScore(), greaterThan(0f));
        SearchHit hit = goodResponse.getHits().getHits()[0];
        assertNotNull(hit.getExplanation());
        assertFalse(goodResponse.getProfileResults().isEmpty());

        assertNull(responses[0].getResponse().getSource());
        assertThat(responses[1].isFailure(), Matchers.is(true));
        assertNotNull(responses[1].getFailureMessage());
        assertThat(responses[1].getFailureMessage(), containsString("json_parse_exception"));
    }

    public void testMultiSearchTemplateAllBad() throws Exception {
        MultiSearchTemplateRequest multiSearchTemplateRequest = new MultiSearchTemplateRequest();

        SearchTemplateRequest badRequest1 = new SearchTemplateRequest();
        badRequest1.setRequest(new SearchRequest("index"));
        badRequest1.setScriptType(ScriptType.INLINE);
        badRequest1.setScript("{ \"query\": { \"match\": { \"num\": {{number}} } } }");
        Map<String, Object> scriptParams = new HashMap<>();
        scriptParams.put("number", "BAD NUMBER");
        badRequest1.setScriptParams(scriptParams);
        multiSearchTemplateRequest.add(badRequest1);

        SearchTemplateRequest badRequest2 = new SearchTemplateRequest();
        badRequest2.setRequest(new SearchRequest("index"));
        badRequest2.setScriptType(ScriptType.INLINE);
        badRequest2.setScript("BAD QUERY TEMPLATE");
        scriptParams = new HashMap<>();
        scriptParams.put("number", "BAD NUMBER");
        badRequest2.setScriptParams(scriptParams);

        multiSearchTemplateRequest.add(badRequest2);

        // The whole HTTP request should fail if no nested search requests are valid
        OpenSearchStatusException exception = expectThrows(
            OpenSearchStatusException.class,
            () -> execute(multiSearchTemplateRequest, highLevelClient()::msearchTemplate, highLevelClient()::msearchTemplateAsync)
        );

        assertEquals(RestStatus.BAD_REQUEST, exception.status());
        assertThat(exception.getMessage(), containsString("no requests added"));
    }

    public void testExplain() throws IOException {
        {
            ExplainRequest explainRequest = new ExplainRequest("index1", "1");
            explainRequest.query(QueryBuilders.matchAllQuery());

            ExplainResponse explainResponse = execute(explainRequest, highLevelClient()::explain, highLevelClient()::explainAsync);

            assertThat(explainResponse.getIndex(), equalTo("index1"));
            assertThat(Integer.valueOf(explainResponse.getId()), equalTo(1));
            assertTrue(explainResponse.isExists());
            assertTrue(explainResponse.isMatch());
            assertTrue(explainResponse.hasExplanation());
            assertThat(explainResponse.getExplanation().getValue(), equalTo(1.0f));
            assertNull(explainResponse.getGetResult());
        }
        {
            ExplainRequest explainRequest = new ExplainRequest("index1", "1");
            explainRequest.query(QueryBuilders.termQuery("field", "value1"));

            ExplainResponse explainResponse = execute(explainRequest, highLevelClient()::explain, highLevelClient()::explainAsync);

            assertThat(explainResponse.getIndex(), equalTo("index1"));
            assertThat(Integer.valueOf(explainResponse.getId()), equalTo(1));
            assertTrue(explainResponse.isExists());
            assertTrue(explainResponse.isMatch());
            assertTrue(explainResponse.hasExplanation());
            assertThat(explainResponse.getExplanation().getValue().floatValue(), greaterThan(0.0f));
            assertNull(explainResponse.getGetResult());
        }
        {
            ExplainRequest explainRequest = new ExplainRequest("index1", "1");
            explainRequest.query(QueryBuilders.termQuery("field", "value2"));

            ExplainResponse explainResponse = execute(explainRequest, highLevelClient()::explain, highLevelClient()::explainAsync);

            assertThat(explainResponse.getIndex(), equalTo("index1"));
            assertThat(Integer.valueOf(explainResponse.getId()), equalTo(1));
            assertTrue(explainResponse.isExists());
            assertFalse(explainResponse.isMatch());
            assertTrue(explainResponse.hasExplanation());
            assertNull(explainResponse.getGetResult());
        }
        {
            ExplainRequest explainRequest = new ExplainRequest("index1", "1");
            explainRequest.query(
                QueryBuilders.boolQuery().must(QueryBuilders.termQuery("field", "value1")).must(QueryBuilders.termQuery("field", "value2"))
            );

            ExplainResponse explainResponse = execute(explainRequest, highLevelClient()::explain, highLevelClient()::explainAsync);

            assertThat(explainResponse.getIndex(), equalTo("index1"));
            assertThat(Integer.valueOf(explainResponse.getId()), equalTo(1));
            assertTrue(explainResponse.isExists());
            assertFalse(explainResponse.isMatch());
            assertTrue(explainResponse.hasExplanation());
            assertThat(explainResponse.getExplanation().getDetails().length, equalTo(2));
            assertNull(explainResponse.getGetResult());
        }
    }

    public void testExplainNonExistent() throws IOException {
        {
            ExplainRequest explainRequest = new ExplainRequest("non_existent_index", "1");
            explainRequest.query(QueryBuilders.matchQuery("field", "value"));
            OpenSearchException exception = expectThrows(
                OpenSearchException.class,
                () -> execute(explainRequest, highLevelClient()::explain, highLevelClient()::explainAsync)
            );
            assertThat(exception.status(), equalTo(RestStatus.NOT_FOUND));
            assertThat(exception.getIndex().getName(), equalTo("non_existent_index"));
            assertThat(
                exception.getDetailedMessage(),
                containsString("OpenSearch exception [type=index_not_found_exception, reason=no such index [non_existent_index]]")
            );
        }
        {
            ExplainRequest explainRequest = new ExplainRequest("index1", "999");
            explainRequest.query(QueryBuilders.matchQuery("field", "value1"));

            ExplainResponse explainResponse = execute(explainRequest, highLevelClient()::explain, highLevelClient()::explainAsync);

            assertThat(explainResponse.getIndex(), equalTo("index1"));
            assertThat(explainResponse.getId(), equalTo("999"));
            assertFalse(explainResponse.isExists());
            assertFalse(explainResponse.isMatch());
            assertFalse(explainResponse.hasExplanation());
            assertNull(explainResponse.getGetResult());
        }
    }

    public void testExplainWithStoredFields() throws IOException {
        {
            ExplainRequest explainRequest = new ExplainRequest("index4", "1");
            explainRequest.query(QueryBuilders.matchAllQuery());
            explainRequest.storedFields(new String[] { "field1" });

            ExplainResponse explainResponse = execute(explainRequest, highLevelClient()::explain, highLevelClient()::explainAsync);

            assertTrue(explainResponse.isExists());
            assertTrue(explainResponse.isMatch());
            assertTrue(explainResponse.hasExplanation());
            assertThat(explainResponse.getExplanation().getValue(), equalTo(1.0f));
            assertTrue(explainResponse.getGetResult().isExists());
            assertThat(explainResponse.getGetResult().getFields().keySet(), equalTo(Collections.singleton("field1")));
            assertThat(explainResponse.getGetResult().getFields().get("field1").getValue().toString(), equalTo("value1"));
            assertTrue(explainResponse.getGetResult().isSourceEmpty());
        }
        {
            ExplainRequest explainRequest = new ExplainRequest("index4", "1");
            explainRequest.query(QueryBuilders.matchAllQuery());
            explainRequest.storedFields(new String[] { "field1", "field2" });

            ExplainResponse explainResponse = execute(explainRequest, highLevelClient()::explain, highLevelClient()::explainAsync);

            assertTrue(explainResponse.isExists());
            assertTrue(explainResponse.isMatch());
            assertTrue(explainResponse.hasExplanation());
            assertThat(explainResponse.getExplanation().getValue(), equalTo(1.0f));
            assertTrue(explainResponse.getGetResult().isExists());
            assertThat(explainResponse.getGetResult().getFields().keySet().size(), equalTo(2));
            assertThat(explainResponse.getGetResult().getFields().get("field1").getValue().toString(), equalTo("value1"));
            assertThat(explainResponse.getGetResult().getFields().get("field2").getValue().toString(), equalTo("value2"));
            assertTrue(explainResponse.getGetResult().isSourceEmpty());
        }
    }

    public void testExplainWithFetchSource() throws IOException {
        {
            ExplainRequest explainRequest = new ExplainRequest("index4", "1");
            explainRequest.query(QueryBuilders.matchAllQuery());
            explainRequest.fetchSourceContext(new FetchSourceContext(true, new String[] { "field1" }, null));

            ExplainResponse explainResponse = execute(explainRequest, highLevelClient()::explain, highLevelClient()::explainAsync);

            assertTrue(explainResponse.isExists());
            assertTrue(explainResponse.isMatch());
            assertTrue(explainResponse.hasExplanation());
            assertThat(explainResponse.getExplanation().getValue(), equalTo(1.0f));
            assertTrue(explainResponse.getGetResult().isExists());
            assertThat(explainResponse.getGetResult().getSource(), equalTo(Collections.singletonMap("field1", "value1")));
        }
        {
            ExplainRequest explainRequest = new ExplainRequest("index4", "1");
            explainRequest.query(QueryBuilders.matchAllQuery());
            explainRequest.fetchSourceContext(new FetchSourceContext(true, null, new String[] { "field2" }));

            ExplainResponse explainResponse = execute(explainRequest, highLevelClient()::explain, highLevelClient()::explainAsync);

            assertTrue(explainResponse.isExists());
            assertTrue(explainResponse.isMatch());
            assertTrue(explainResponse.hasExplanation());
            assertThat(explainResponse.getExplanation().getValue(), equalTo(1.0f));
            assertTrue(explainResponse.getGetResult().isExists());
            assertEquals(2, explainResponse.getGetResult().getSource().size());
            assertThat(explainResponse.getGetResult().getSource().get("field1"), equalTo("value1"));
        }
    }

    public void testExplainWithAliasFilter() throws IOException {
        ExplainRequest explainRequest = new ExplainRequest("alias4", "1");
        explainRequest.query(QueryBuilders.matchAllQuery());

        ExplainResponse explainResponse = execute(explainRequest, highLevelClient()::explain, highLevelClient()::explainAsync);

        assertTrue(explainResponse.isExists());
        assertFalse(explainResponse.isMatch());
    }

    public void testFieldCaps() throws IOException {
        FieldCapabilitiesRequest request = new FieldCapabilitiesRequest().indices("index1", "index2").fields("rating", "field");

        FieldCapabilitiesResponse response = execute(request, highLevelClient()::fieldCaps, highLevelClient()::fieldCapsAsync);

        assertThat(response.getIndices(), arrayContaining("index1", "index2"));

        // Check the capabilities for the 'rating' field.
        assertTrue(response.get().containsKey("rating"));
        Map<String, FieldCapabilities> ratingResponse = response.getField("rating");
        assertEquals(2, ratingResponse.size());

        FieldCapabilities expectedKeywordCapabilities = new FieldCapabilities(
            "rating",
            "keyword",
            true,
            true,
            new String[] { "index2" },
            null,
            null,
            Collections.emptyMap()
        );
        assertEquals(expectedKeywordCapabilities, ratingResponse.get("keyword"));

        FieldCapabilities expectedLongCapabilities = new FieldCapabilities(
            "rating",
            "long",
            true,
            true,
            new String[] { "index1" },
            null,
            null,
            Collections.emptyMap()
        );
        assertEquals(expectedLongCapabilities, ratingResponse.get("long"));

        // Check the capabilities for the 'field' field.
        assertTrue(response.get().containsKey("field"));
        Map<String, FieldCapabilities> fieldResponse = response.getField("field");
        assertEquals(1, fieldResponse.size());

        FieldCapabilities expectedTextCapabilities = new FieldCapabilities(
            "field",
            "text",
            true,
            false,
            null,
            null,
            null,
            Collections.emptyMap()
        );
        assertEquals(expectedTextCapabilities, fieldResponse.get("text"));
    }

    public void testFieldCapsWithNonExistentFields() throws IOException {
        FieldCapabilitiesRequest request = new FieldCapabilitiesRequest().indices("index2").fields("nonexistent");

        FieldCapabilitiesResponse response = execute(request, highLevelClient()::fieldCaps, highLevelClient()::fieldCapsAsync);
        assertTrue(response.get().isEmpty());
    }

    public void testFieldCapsWithNonExistentIndices() {
        FieldCapabilitiesRequest request = new FieldCapabilitiesRequest().indices("non-existent").fields("rating");

        OpenSearchException exception = expectThrows(
            OpenSearchException.class,
            () -> execute(request, highLevelClient()::fieldCaps, highLevelClient()::fieldCapsAsync)
        );
        assertEquals(RestStatus.NOT_FOUND, exception.status());
    }

    private static void assertSearchHeader(SearchResponse searchResponse) {
        assertThat(searchResponse.getTook().nanos(), greaterThanOrEqualTo(0L));
        assertEquals(0, searchResponse.getFailedShards());
        assertThat(searchResponse.getTotalShards(), greaterThan(0));
        assertEquals(searchResponse.getTotalShards(), searchResponse.getSuccessfulShards());
        assertEquals(0, searchResponse.getShardFailures().length);
        assertEquals(SearchResponse.Clusters.EMPTY, searchResponse.getClusters());
    }

    public void testCountOneIndexNoQuery() throws IOException {
        CountRequest countRequest = new CountRequest("index");
        CountResponse countResponse = execute(countRequest, highLevelClient()::count, highLevelClient()::countAsync);
        assertCountHeader(countResponse);
        assertEquals(5, countResponse.getCount());
    }

    public void testCountMultipleIndicesNoQuery() throws IOException {
        CountRequest countRequest = new CountRequest("index", "index1");
        CountResponse countResponse = execute(countRequest, highLevelClient()::count, highLevelClient()::countAsync);
        assertCountHeader(countResponse);
        assertEquals(7, countResponse.getCount());
    }

    public void testCountAllIndicesNoQuery() throws IOException {
        CountRequest countRequest = new CountRequest();
        CountResponse countResponse = execute(countRequest, highLevelClient()::count, highLevelClient()::countAsync);
        assertCountHeader(countResponse);
        assertEquals(12, countResponse.getCount());
    }

    public void testCountOneIndexMatchQuery() throws IOException {
        CountRequest countRequest = new CountRequest("index");
        countRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("num", 10)));
        CountResponse countResponse = execute(countRequest, highLevelClient()::count, highLevelClient()::countAsync);
        assertCountHeader(countResponse);
        assertEquals(1, countResponse.getCount());
    }

    public void testCountMultipleIndicesMatchQueryUsingConstructor() throws IOException {
        CountRequest countRequest;
        if (randomBoolean()) {
            SearchSourceBuilder sourceBuilder = new SearchSourceBuilder().query(new MatchQueryBuilder("field", "value1"));
            countRequest = new CountRequest(new String[] { "index1", "index2", "index3" }, sourceBuilder);
        } else {
            QueryBuilder query = new MatchQueryBuilder("field", "value1");
            countRequest = new CountRequest(new String[] { "index1", "index2", "index3" }, query);
        }
        CountResponse countResponse = execute(countRequest, highLevelClient()::count, highLevelClient()::countAsync);
        assertCountHeader(countResponse);
        assertEquals(3, countResponse.getCount());

    }

    public void testCountMultipleIndicesMatchQuery() throws IOException {
        CountRequest countRequest = new CountRequest("index1", "index2", "index3");
        if (randomBoolean()) {
            countRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("field", "value1")));
        } else {
            countRequest.query(new MatchQueryBuilder("field", "value1"));
        }
        CountResponse countResponse = execute(countRequest, highLevelClient()::count, highLevelClient()::countAsync);
        assertCountHeader(countResponse);
        assertEquals(3, countResponse.getCount());
    }

    public void testCountAllIndicesMatchQuery() throws IOException {
        CountRequest countRequest = new CountRequest();
        countRequest.source(new SearchSourceBuilder().query(new MatchQueryBuilder("field", "value1")));
        CountResponse countResponse = execute(countRequest, highLevelClient()::count, highLevelClient()::countAsync);
        assertCountHeader(countResponse);
        assertEquals(3, countResponse.getCount());
    }

    private static void assertCountHeader(CountResponse countResponse) {
        assertEquals(0, countResponse.getSkippedShards());
        assertEquals(0, countResponse.getFailedShards());
        assertThat(countResponse.getTotalShards(), greaterThan(0));
        assertEquals(countResponse.getTotalShards(), countResponse.getSuccessfulShards());
        assertEquals(0, countResponse.getShardFailures().length);
    }
}
