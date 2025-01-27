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

package org.opensearch.index.mapper;

import com.carrotsearch.randomizedtesting.annotations.Name;
import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.OpenSearchException;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;

public class TokenCountFieldMapperIntegrationIT extends OpenSearchIntegTestCase {
    @ParametersFactory
    public static Iterable<Object[]> buildParameters() {
        List<Object[]> parameters = new ArrayList<>();
        for (boolean storeCountedFields : new boolean[] { true, false }) {
            for (boolean loadCountedFields : new boolean[] { true, false }) {
                parameters.add(new Object[] { storeCountedFields, loadCountedFields });
            }
        }
        return parameters;
    }

    private final boolean storeCountedFields;
    private final boolean loadCountedFields;

    public TokenCountFieldMapperIntegrationIT(
        @Name("storeCountedFields") boolean storeCountedFields,
        @Name("loadCountedFields") boolean loadCountedFields
    ) {
        this.storeCountedFields = storeCountedFields;
        this.loadCountedFields = loadCountedFields;
    }

    /**
     * It is possible to get the token count in a search response.
     */
    public void testSearchReturnsTokenCount() throws IOException {
        init();

        assertSearchReturns(searchById("single"), "single");
        assertSearchReturns(searchById("bulk1"), "bulk1");
        assertSearchReturns(searchById("bulk2"), "bulk2");
        assertSearchReturns(searchById("multi"), "multi");
        assertSearchReturns(searchById("multibulk1"), "multibulk1");
        assertSearchReturns(searchById("multibulk2"), "multibulk2");
    }

    /**
     * It is possible to search by token count.
     */
    public void testSearchByTokenCount() throws IOException {
        init();

        assertSearchReturns(searchByNumericRange(4, 4).get(), "single");
        assertSearchReturns(searchByNumericRange(10, 10).get(), "multibulk2");
        assertSearchReturns(searchByNumericRange(7, 10).get(), "multi", "multibulk1", "multibulk2");
        assertSearchReturns(searchByNumericRange(1, 10).get(), "single", "bulk1", "bulk2", "multi", "multibulk1", "multibulk2");
        assertSearchReturns(searchByNumericRange(12, 12).get());
    }

    /**
     * It is possible to search by token count.
     */
    public void testFacetByTokenCount() throws IOException {
        init();

        String facetField = randomFrom(Arrays.asList("foo.token_count", "foo.token_count_unstored", "foo.token_count_with_doc_values"));
        SearchResponse result = searchByNumericRange(1, 10).addAggregation(AggregationBuilders.terms("facet").field(facetField)).get();
        assertSearchReturns(result, "single", "bulk1", "bulk2", "multi", "multibulk1", "multibulk2");
        assertThat(result.getAggregations().asList().size(), equalTo(1));
        Terms terms = (Terms) result.getAggregations().asList().get(0);
        assertThat(terms.getBuckets().size(), equalTo(9));
    }

    private void init() throws IOException {
        Settings.Builder settings = Settings.builder();
        settings.put(indexSettings());
        settings.put("index.analysis.analyzer.mock_english.tokenizer", "standard");
        settings.put("index.analysis.analyzer.mock_english.filter", "stop");
        prepareCreate("test").setSettings(settings)
            .setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("foo")
                    .field("type", "text")
                    .field("store", storeCountedFields)
                    .field("analyzer", "simple")
                    .startObject("fields")
                    .startObject("token_count")
                    .field("type", "token_count")
                    .field("analyzer", "standard")
                    .field("store", true)
                    .endObject()
                    .startObject("token_count_unstored")
                    .field("type", "token_count")
                    .field("analyzer", "standard")
                    .endObject()
                    .startObject("token_count_with_doc_values")
                    .field("type", "token_count")
                    .field("analyzer", "standard")
                    .field("doc_values", true)
                    .endObject()
                    .startObject("token_count_without_position_increments")
                    .field("type", "token_count")
                    .field("analyzer", "mock_english")
                    .field("enable_position_increments", false)
                    .field("store", true)
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .get();
        ensureGreen();

        assertEquals(DocWriteResponse.Result.CREATED, prepareIndex("single", "I have four terms").get().getResult());
        BulkResponse bulk = client().prepareBulk()
            .add(prepareIndex("bulk1", "bulk three terms"))
            .add(prepareIndex("bulk2", "this has five bulk terms"))
            .get();
        assertFalse(bulk.buildFailureMessage(), bulk.hasFailures());
        assertEquals(
            DocWriteResponse.Result.CREATED,
            prepareIndex("multi", "two terms", "wow now I have seven lucky terms").get().getResult()
        );
        bulk = client().prepareBulk()
            .add(prepareIndex("multibulk1", "one", "oh wow now I have eight unlucky terms"))
            .add(prepareIndex("multibulk2", "six is a bunch of terms", "ten!  ten terms is just crazy!  too many too count!"))
            .get();
        assertFalse(bulk.buildFailureMessage(), bulk.hasFailures());

        assertThat(refresh().getFailedShards(), equalTo(0));
    }

    private IndexRequestBuilder prepareIndex(String id, String... texts) throws IOException {
        return client().prepareIndex("test").setId(id).setSource("foo", texts);
    }

    private SearchResponse searchById(String id) {
        return prepareSearch().setQuery(QueryBuilders.termQuery("_id", id)).get();
    }

    private SearchRequestBuilder searchByNumericRange(int low, int high) {
        return prepareSearch().setQuery(
            QueryBuilders.rangeQuery(
                randomFrom(Arrays.asList("foo.token_count", "foo.token_count_unstored", "foo.token_count_with_doc_values"))
            ).gte(low).lte(high)
        );
    }

    private SearchRequestBuilder prepareSearch() {
        SearchRequestBuilder request = client().prepareSearch("test");
        request.addStoredField("foo.token_count");
        request.addStoredField("foo.token_count_without_position_increments");
        if (loadCountedFields) {
            request.addStoredField("foo");
        }
        return request;
    }

    private void assertSearchReturns(SearchResponse result, String... ids) {
        assertThat(result.getHits().getTotalHits().value(), equalTo((long) ids.length));
        assertThat(result.getHits().getHits().length, equalTo(ids.length));
        List<String> foundIds = new ArrayList<>();
        for (SearchHit hit : result.getHits()) {
            foundIds.add(hit.getId());
        }
        assertThat(foundIds, containsInAnyOrder(ids));
        for (SearchHit hit : result.getHits()) {
            String id = hit.getId();
            if (id.equals("single")) {
                assertSearchHit(hit, new int[] { 4 }, new int[] { 4 });
            } else if (id.equals("bulk1")) {
                assertSearchHit(hit, new int[] { 3 }, new int[] { 3 });
            } else if (id.equals("bulk2")) {
                assertSearchHit(hit, new int[] { 5 }, new int[] { 4 });
            } else if (id.equals("multi")) {
                assertSearchHit(hit, new int[] { 2, 7 }, new int[] { 2, 7 });
            } else if (id.equals("multibulk1")) {
                assertSearchHit(hit, new int[] { 1, 8 }, new int[] { 1, 8 });
            } else if (id.equals("multibulk2")) {
                assertSearchHit(hit, new int[] { 6, 10 }, new int[] { 3, 9 });
            } else {
                throw new OpenSearchException("Unexpected response!");
            }
        }
    }

    private void assertSearchHit(SearchHit hit, int[] standardTermCounts, int[] englishTermCounts) {
        assertThat(hit.field("foo.token_count"), not(nullValue()));
        assertThat(hit.field("foo.token_count").getValues().size(), equalTo(standardTermCounts.length));
        for (int i = 0; i < standardTermCounts.length; i++) {
            assertThat(hit.field("foo.token_count").getValues().get(i), equalTo(standardTermCounts[i]));
        }

        assertThat(hit.field("foo.token_count_without_position_increments"), not(nullValue()));
        assertThat(hit.field("foo.token_count_without_position_increments").getValues().size(), equalTo(englishTermCounts.length));
        for (int i = 0; i < englishTermCounts.length; i++) {
            assertThat(hit.field("foo.token_count_without_position_increments").getValues().get(i), equalTo(englishTermCounts[i]));
        }

        if (loadCountedFields && storeCountedFields) {
            assertThat(hit.field("foo").getValues().size(), equalTo(standardTermCounts.length));
        }
    }
}
