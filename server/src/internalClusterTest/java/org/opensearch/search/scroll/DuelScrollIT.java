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

package org.opensearch.search.scroll;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchType;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.sort.SortBuilder;
import org.opensearch.search.sort.SortBuilders;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.equalTo;

public class DuelScrollIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {
    public DuelScrollIT(Settings settings) {
        super(settings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    public void testDuelQueryThenFetch() throws Exception {
        TestContext context = create(SearchType.DFS_QUERY_THEN_FETCH, SearchType.QUERY_THEN_FETCH);

        SearchResponse control = client().prepareSearch("index")
            .setSearchType(context.searchType)
            .addSort(context.sort)
            .setSize(context.numDocs)
            .get();
        assertNoFailures(control);
        SearchHits sh = control.getHits();
        assertThat(sh.getTotalHits().value(), equalTo((long) context.numDocs));
        assertThat(sh.getHits().length, equalTo(context.numDocs));

        SearchResponse searchScrollResponse = client().prepareSearch("index")
            .setSearchType(context.searchType)
            .addSort(context.sort)
            .setSize(context.scrollRequestSize)
            .setScroll("10m")
            .get();

        assertNoFailures(searchScrollResponse);
        assertThat(searchScrollResponse.getHits().getTotalHits().value(), equalTo((long) context.numDocs));
        assertThat(searchScrollResponse.getHits().getHits().length, equalTo(context.scrollRequestSize));

        int counter = 0;
        for (SearchHit hit : searchScrollResponse.getHits()) {
            assertThat(hit.getSortValues()[0], equalTo(sh.getAt(counter++).getSortValues()[0]));
        }

        int iter = 1;
        String scrollId = searchScrollResponse.getScrollId();
        while (true) {
            searchScrollResponse = client().prepareSearchScroll(scrollId).setScroll("10m").get();
            assertNoFailures(searchScrollResponse);
            assertThat(searchScrollResponse.getHits().getTotalHits().value(), equalTo((long) context.numDocs));
            if (searchScrollResponse.getHits().getHits().length == 0) {
                break;
            }

            int expectedLength;
            int scrollSlice = ++iter * context.scrollRequestSize;
            if (scrollSlice <= context.numDocs) {
                expectedLength = context.scrollRequestSize;
            } else {
                expectedLength = context.scrollRequestSize - (scrollSlice - context.numDocs);
            }
            assertThat(searchScrollResponse.getHits().getHits().length, equalTo(expectedLength));
            for (SearchHit hit : searchScrollResponse.getHits()) {
                assertThat(hit.getSortValues()[0], equalTo(sh.getAt(counter++).getSortValues()[0]));
            }
            scrollId = searchScrollResponse.getScrollId();
        }

        assertThat(counter, equalTo(context.numDocs));
        clearScroll(scrollId);
    }

    private TestContext create(SearchType... searchTypes) throws Exception {
        assertAcked(
            prepareCreate("index").setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("field1")
                    .field("type", "long")
                    .endObject()
                    .startObject("field2")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("nested")
                    .field("type", "nested")
                    .startObject("properties")
                    .startObject("field3")
                    .field("type", "long")
                    .endObject()
                    .startObject("field4")
                    .field("type", "keyword")
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );

        int numDocs = 2 + randomInt(512);
        int scrollRequestSize = randomIntBetween(1, rarely() ? numDocs : numDocs / 2);
        boolean unevenRouting = randomBoolean();

        int numMissingDocs = scaledRandomIntBetween(0, numDocs / 100);
        final Set<Integer> missingDocs = new HashSet<>(numMissingDocs);
        for (int i = 0; i < numMissingDocs; i++) {
            while (!missingDocs.add(randomInt(numDocs))) {
            }
        }

        for (int i = 1; i <= numDocs; i++) {
            IndexRequestBuilder indexRequestBuilder = client().prepareIndex("index").setId(String.valueOf(i));
            if (missingDocs.contains(i)) {
                indexRequestBuilder.setSource("x", "y");
            } else {
                indexRequestBuilder.setSource(
                    jsonBuilder().startObject()
                        .field("field1", i)
                        .field("field2", String.valueOf(i))
                        .startObject("nested")
                        .field("field3", i)
                        .field("field4", String.valueOf(i))
                        .endObject()
                        .endObject()
                );
            }

            if (unevenRouting && randomInt(3) <= 2) {
                indexRequestBuilder.setRouting("a");
            }
            indexRandom(false, indexRequestBuilder);
        }
        refresh();

        final SortBuilder sort;
        if (randomBoolean()) {
            if (randomBoolean()) {
                sort = SortBuilders.fieldSort("field1").missing(1);
            } else {
                sort = SortBuilders.fieldSort("field2").missing("1");
            }
        } else {
            if (randomBoolean()) {
                sort = SortBuilders.fieldSort("nested.field3").missing(1);
            } else {
                sort = SortBuilders.fieldSort("nested.field4").missing("1");
            }
        }
        sort.order(randomBoolean() ? SortOrder.ASC : SortOrder.DESC);

        SearchType searchType = RandomPicks.randomFrom(random(), Arrays.asList(searchTypes));

        logger.info("numDocs={}, scrollRequestSize={}, sort={}, searchType={}", numDocs, scrollRequestSize, sort, searchType);
        return new TestContext(numDocs, scrollRequestSize, sort, searchType);
    }

    class TestContext {

        final int numDocs;
        final int scrollRequestSize;
        final SortBuilder sort;
        final SearchType searchType;

        TestContext(int numDocs, int scrollRequestSize, SortBuilder sort, SearchType searchType) {
            this.numDocs = numDocs;
            this.scrollRequestSize = scrollRequestSize;
            this.sort = sort;
            this.searchType = searchType;
        }
    }

    private int createIndex(boolean singleShard) throws Exception {
        Settings.Builder settings = Settings.builder();
        if (singleShard) {
            settings.put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1);
        }
        // no replicas, as they might be ordered differently
        settings.put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0);
        // we need to control refreshes as they might take different merges into account
        settings.put("index.refresh_interval", -1);

        assertAcked(prepareCreate("test").setSettings(settings.build()).get());
        final int numDocs = randomIntBetween(10, 200);

        IndexRequestBuilder[] builders = new IndexRequestBuilder[numDocs];
        for (int i = 0; i < numDocs; ++i) {
            builders[i] = client().prepareIndex("test").setId(Integer.toString(i)).setSource("foo", random().nextBoolean());
        }
        indexRandom(true, builders);
        return numDocs;
    }

    private void testDuelIndexOrder(SearchType searchType, boolean trackScores, int numDocs) throws Exception {
        final int size = scaledRandomIntBetween(5, numDocs + 5);
        final SearchResponse control = client().prepareSearch("test")
            .setSearchType(searchType)
            .setSize(numDocs)
            .setQuery(QueryBuilders.matchQuery("foo", "true"))
            .addSort(SortBuilders.fieldSort("_doc"))
            .setTrackScores(trackScores)
            .get();
        assertNoFailures(control);

        SearchResponse scroll = client().prepareSearch("test")
            .setSearchType(searchType)
            .setSize(size)
            .setQuery(QueryBuilders.matchQuery("foo", "true"))
            .addSort(SortBuilders.fieldSort("_doc"))
            .setTrackScores(trackScores)
            .setScroll("10m")
            .get();

        int scrollDocs = 0;
        try {
            while (true) {
                assertNoFailures(scroll);
                assertEquals(control.getHits().getTotalHits().value(), scroll.getHits().getTotalHits().value());
                assertEquals(control.getHits().getMaxScore(), scroll.getHits().getMaxScore(), 0.01f);
                if (scroll.getHits().getHits().length == 0) {
                    break;
                }
                for (int i = 0; i < scroll.getHits().getHits().length; ++i) {
                    SearchHit controlHit = control.getHits().getAt(scrollDocs + i);
                    SearchHit scrollHit = scroll.getHits().getAt(i);
                    assertEquals(controlHit.getId(), scrollHit.getId());
                }
                scrollDocs += scroll.getHits().getHits().length;
                scroll = client().prepareSearchScroll(scroll.getScrollId()).setScroll("10m").get();
            }
            assertEquals(control.getHits().getTotalHits().value(), scrollDocs);
        } catch (AssertionError e) {
            logger.info("Control:\n{}", control);
            logger.info("Scroll size={}, from={}:\n{}", size, scrollDocs, scroll);
            throw e;
        } finally {
            clearScroll(scroll.getScrollId());
        }
    }

    public void testDuelIndexOrderQueryThenFetch() throws Exception {
        final SearchType searchType = RandomPicks.randomFrom(
            random(),
            Arrays.asList(SearchType.QUERY_THEN_FETCH, SearchType.DFS_QUERY_THEN_FETCH)
        );
        final int numDocs = createIndex(false);
        testDuelIndexOrder(searchType, false, numDocs);
        testDuelIndexOrder(searchType, true, numDocs);
    }
}
