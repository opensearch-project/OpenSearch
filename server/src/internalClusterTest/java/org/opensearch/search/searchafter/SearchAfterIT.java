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

package org.opensearch.search.searchafter;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.CreatePitAction;
import org.opensearch.action.search.CreatePitRequest;
import org.opensearch.action.search.CreatePitResponse;
import org.opensearch.action.search.SearchPhaseExecutionException;
import org.opensearch.action.search.SearchRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.common.UUIDs;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.PointInTimeBuilder;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;
import org.hamcrest.Matchers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SearchAfterIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {
    private static final String INDEX_NAME = "test";
    private static final int NUM_DOCS = 100;

    public SearchAfterIT(Settings settings) {
        super(settings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    public void testsShouldFail() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").setMapping("field1", "type=long", "field2", "type=keyword").get());
        ensureGreen();
        indexRandom(true, client().prepareIndex("test").setId("0").setSource("field1", 0, "field2", "toto"));
        {
            SearchPhaseExecutionException e = expectThrows(
                SearchPhaseExecutionException.class,
                () -> client().prepareSearch("test")
                    .addSort("field1", SortOrder.ASC)
                    .setQuery(matchAllQuery())
                    .searchAfter(new Object[] { 0 })
                    .setScroll("1m")
                    .get()
            );
            assertTrue(e.shardFailures().length > 0);
            for (ShardSearchFailure failure : e.shardFailures()) {
                assertThat(failure.toString(), containsString("`search_after` cannot be used in a scroll context."));
            }
        }

        {
            SearchPhaseExecutionException e = expectThrows(
                SearchPhaseExecutionException.class,
                () -> client().prepareSearch("test")
                    .addSort("field1", SortOrder.ASC)
                    .setQuery(matchAllQuery())
                    .searchAfter(new Object[] { 0 })
                    .setFrom(10)
                    .get()
            );
            assertTrue(e.shardFailures().length > 0);
            for (ShardSearchFailure failure : e.shardFailures()) {
                assertThat(failure.toString(), containsString("`from` parameter must be set to 0 when `search_after` is used."));
            }
        }

        {
            SearchPhaseExecutionException e = expectThrows(
                SearchPhaseExecutionException.class,
                () -> client().prepareSearch("test").setQuery(matchAllQuery()).searchAfter(new Object[] { 0.75f }).get()
            );
            assertTrue(e.shardFailures().length > 0);
            for (ShardSearchFailure failure : e.shardFailures()) {
                assertThat(failure.toString(), containsString("Sort must contain at least one field."));
            }
        }

        {
            SearchPhaseExecutionException e = expectThrows(
                SearchPhaseExecutionException.class,
                () -> client().prepareSearch("test")
                    .addSort("field2", SortOrder.DESC)
                    .addSort("field1", SortOrder.ASC)
                    .setQuery(matchAllQuery())
                    .searchAfter(new Object[] { 1 })
                    .get()
            );
            assertTrue(e.shardFailures().length > 0);
            for (ShardSearchFailure failure : e.shardFailures()) {
                assertThat(failure.toString(), containsString("search_after has 1 value(s) but sort has 2."));
            }
        }

        {
            SearchPhaseExecutionException e = expectThrows(
                SearchPhaseExecutionException.class,
                () -> client().prepareSearch("test")
                    .setQuery(matchAllQuery())
                    .addSort("field1", SortOrder.ASC)
                    .searchAfter(new Object[] { 1, 2 })
                    .get()
            );
            for (ShardSearchFailure failure : e.shardFailures()) {
                assertTrue(e.shardFailures().length > 0);
                assertThat(failure.toString(), containsString("search_after has 2 value(s) but sort has 1."));
            }
        }

        {
            SearchPhaseExecutionException e = expectThrows(
                SearchPhaseExecutionException.class,
                () -> client().prepareSearch("test")
                    .setQuery(matchAllQuery())
                    .addSort("field1", SortOrder.ASC)
                    .searchAfter(new Object[] { "toto" })
                    .get()
            );
            assertTrue(e.shardFailures().length > 0);
            for (ShardSearchFailure failure : e.shardFailures()) {
                assertThat(failure.toString(), containsString("Failed to parse search_after value for field [field1]."));
            }
        }
    }

    public void testPitWithSearchAfter() throws Exception {
        assertAcked(client().admin().indices().prepareCreate("test").setMapping("field1", "type=long", "field2", "type=keyword").get());
        ensureGreen();
        indexRandom(
            true,
            client().prepareIndex("test").setId("0").setSource("field1", 0),
            client().prepareIndex("test").setId("1").setSource("field1", 100, "field2", "toto"),
            client().prepareIndex("test").setId("2").setSource("field1", 101),
            client().prepareIndex("test").setId("3").setSource("field1", 99)
        );

        CreatePitRequest request = new CreatePitRequest(TimeValue.timeValueDays(1), true);
        request.setIndices(new String[] { "test" });
        ActionFuture<CreatePitResponse> execute = client().execute(CreatePitAction.INSTANCE, request);
        CreatePitResponse pitResponse = execute.get();
        SearchResponse sr = client().prepareSearch()
            .addSort("field1", SortOrder.ASC)
            .setQuery(matchAllQuery())
            .searchAfter(new Object[] { 99 })
            .setPointInTime(new PointInTimeBuilder(pitResponse.getId()))
            .get();
        assertEquals(2, sr.getHits().getHits().length);
        sr = client().prepareSearch()
            .addSort("field1", SortOrder.ASC)
            .setQuery(matchAllQuery())
            .searchAfter(new Object[] { 100 })
            .setPointInTime(new PointInTimeBuilder(pitResponse.getId()))
            .get();
        assertEquals(1, sr.getHits().getHits().length);
        sr = client().prepareSearch()
            .addSort("field1", SortOrder.ASC)
            .setQuery(matchAllQuery())
            .searchAfter(new Object[] { 0 })
            .setPointInTime(new PointInTimeBuilder(pitResponse.getId()))
            .get();
        assertEquals(3, sr.getHits().getHits().length);
        /*
          Add new data and assert PIT results remain the same and normal search results gets refreshed
         */
        indexRandom(true, client().prepareIndex("test").setId("4").setSource("field1", 102));
        sr = client().prepareSearch()
            .addSort("field1", SortOrder.ASC)
            .setQuery(matchAllQuery())
            .searchAfter(new Object[] { 0 })
            .setPointInTime(new PointInTimeBuilder(pitResponse.getId()))
            .get();
        assertEquals(3, sr.getHits().getHits().length);
        sr = client().prepareSearch().addSort("field1", SortOrder.ASC).setQuery(matchAllQuery()).searchAfter(new Object[] { 0 }).get();
        assertEquals(4, sr.getHits().getHits().length);
        client().admin().indices().prepareDelete("test").get();
    }

    public void testWithNullStrings() throws InterruptedException {
        assertAcked(client().admin().indices().prepareCreate("test").setMapping("field2", "type=keyword").get());
        ensureGreen();
        indexRandom(
            true,
            client().prepareIndex("test").setId("0").setSource("field1", 0),
            client().prepareIndex("test").setId("1").setSource("field1", 100, "field2", "toto")
        );
        SearchResponse searchResponse = client().prepareSearch("test")
            .addSort("field1", SortOrder.ASC)
            .addSort("field2", SortOrder.ASC)
            .setQuery(matchAllQuery())
            .searchAfter(new Object[] { 0, null })
            .get();
        assertThat(searchResponse.getHits().getTotalHits().value(), Matchers.equalTo(2L));
        assertThat(searchResponse.getHits().getHits().length, Matchers.equalTo(1));
        assertThat(searchResponse.getHits().getHits()[0].getSourceAsMap().get("field1"), Matchers.equalTo(100));
        assertThat(searchResponse.getHits().getHits()[0].getSourceAsMap().get("field2"), Matchers.equalTo("toto"));
    }

    public void testWithSimpleTypes() throws Exception {
        int numFields = randomInt(20) + 1;
        int[] types = new int[numFields - 1];
        for (int i = 0; i < numFields - 1; i++) {
            types[i] = randomInt(6);
        }
        List<List> documents = new ArrayList<>();
        for (int i = 0; i < NUM_DOCS; i++) {
            List values = new ArrayList<>();
            for (int type : types) {
                switch (type) {
                    case 0:
                        values.add(randomBoolean());
                        break;
                    case 1:
                        values.add(randomByte());
                        break;
                    case 2:
                        values.add(randomShort());
                        break;
                    case 3:
                        values.add(randomInt());
                        break;
                    case 4:
                        values.add(randomFloat());
                        break;
                    case 5:
                        values.add(randomDouble());
                        break;
                    case 6:
                        values.add(randomAlphaOfLengthBetween(5, 20));
                        break;
                }
            }
            values.add(UUIDs.randomBase64UUID());
            documents.add(values);
        }
        int reqSize = randomInt(NUM_DOCS - 1);
        if (reqSize == 0) {
            reqSize = 1;
        }
        assertSearchFromWithSortValues(INDEX_NAME, documents, reqSize);
    }

    private static class ListComparator implements Comparator<List> {
        @Override
        public int compare(List o1, List o2) {
            if (o1.size() > o2.size()) {
                return 1;
            }

            if (o2.size() > o1.size()) {
                return -1;
            }

            for (int i = 0; i < o1.size(); i++) {
                if (!(o1.get(i) instanceof Comparable)) {
                    throw new RuntimeException(o1.get(i).getClass() + " is not comparable");
                }
                Object cmp1 = o1.get(i);
                Object cmp2 = o2.get(i);
                int cmp = ((Comparable) cmp1).compareTo(cmp2);
                if (cmp != 0) {
                    return cmp;
                }
            }
            return 0;
        }
    }

    private ListComparator LST_COMPARATOR = new ListComparator();

    private void assertSearchFromWithSortValues(String indexName, List<List> documents, int reqSize) throws Exception {
        int numFields = documents.get(0).size();
        {
            createIndexMappingsFromObjectType(indexName, documents.get(0));
            List<IndexRequestBuilder> requests = new ArrayList<>();
            for (int i = 0; i < documents.size(); i++) {
                XContentBuilder builder = jsonBuilder();
                assertThat(documents.get(i).size(), Matchers.equalTo(numFields));
                builder.startObject();
                for (int j = 0; j < numFields; j++) {
                    builder.field("field" + Integer.toString(j), documents.get(i).get(j));
                }
                builder.endObject();
                requests.add(client().prepareIndex(INDEX_NAME).setId(Integer.toString(i)).setSource(builder));
            }
            indexRandom(true, requests);
        }

        Collections.sort(documents, LST_COMPARATOR);
        int offset = 0;
        Object[] sortValues = null;
        while (offset < documents.size()) {
            SearchRequestBuilder req = client().prepareSearch(indexName);
            for (int i = 0; i < documents.get(0).size(); i++) {
                req.addSort("field" + Integer.toString(i), SortOrder.ASC);
            }
            req.setQuery(matchAllQuery()).setSize(reqSize);
            if (sortValues != null) {
                req.searchAfter(sortValues);
            }
            SearchResponse searchResponse = req.get();
            for (SearchHit hit : searchResponse.getHits()) {
                List toCompare = convertSortValues(documents.get(offset++));
                assertThat(LST_COMPARATOR.compare(toCompare, Arrays.asList(hit.getSortValues())), equalTo(0));
            }
            sortValues = searchResponse.getHits().getHits()[searchResponse.getHits().getHits().length - 1].getSortValues();
        }
    }

    private void createIndexMappingsFromObjectType(String indexName, List<Object> types) {
        CreateIndexRequestBuilder indexRequestBuilder = client().admin().indices().prepareCreate(indexName);
        List<String> mappings = new ArrayList<>();
        int numFields = types.size();
        for (int i = 0; i < numFields; i++) {
            Class type = types.get(i).getClass();
            if (type == Integer.class) {
                mappings.add("field" + Integer.toString(i));
                mappings.add("type=integer");
            } else if (type == Long.class) {
                mappings.add("field" + Integer.toString(i));
                mappings.add("type=long");
            } else if (type == Float.class) {
                mappings.add("field" + Integer.toString(i));
                mappings.add("type=float");
            } else if (type == Double.class) {
                mappings.add("field" + Integer.toString(i));
                mappings.add("type=double");
            } else if (type == Byte.class) {
                mappings.add("field" + Integer.toString(i));
                mappings.add("type=byte");
            } else if (type == Short.class) {
                mappings.add("field" + Integer.toString(i));
                mappings.add("type=short");
            } else if (type == Boolean.class) {
                mappings.add("field" + Integer.toString(i));
                mappings.add("type=boolean");
            } else if (types.get(i) instanceof String) {
                mappings.add("field" + Integer.toString(i));
                mappings.add("type=keyword");
            } else {
                fail("Can't match type [" + type + "]");
            }
        }
        indexRequestBuilder.setMapping(mappings.toArray(new String[0])).get();
        ensureGreen();
    }

    // Convert Integer, Short, Byte and Boolean to Int in order to match the conversion done
    // by the internal hits when populating the sort values.
    private List<Object> convertSortValues(List<Object> sortValues) {
        List<Object> converted = new ArrayList<>();
        for (int i = 0; i < sortValues.size(); i++) {
            Object from = sortValues.get(i);
            if (from instanceof Short) {
                converted.add(((Short) from).intValue());
            } else if (from instanceof Byte) {
                converted.add(((Byte) from).intValue());
            } else if (from instanceof Boolean) {
                boolean b = (boolean) from;
                if (b) {
                    converted.add(1);
                } else {
                    converted.add(0);
                }
            } else {
                converted.add(from);
            }
        }
        return converted;
    }
}
