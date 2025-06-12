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

package org.opensearch.index;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.apache.lucene.search.Sort;
import org.apache.lucene.search.SortField;
import org.apache.lucene.search.SortedNumericSortField;
import org.apache.lucene.search.SortedSetSortField;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.sort.SortOrder;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.hamcrest.Matchers.containsString;

public class IndexSortIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {
    private static final XContentBuilder TEST_MAPPING = createTestMapping();
    private static final XContentBuilder NESTED_TEST_MAPPING = createNestedTestMapping();

    public IndexSortIT(Settings staticSettings) {
        super(staticSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    private static XContentBuilder createTestMapping() {
        try {
            return jsonBuilder().startObject()
                .startObject("properties")
                .startObject("date")
                .field("type", "date")
                .endObject()
                .startObject("numeric")
                .field("type", "integer")
                .field("doc_values", false)
                .endObject()
                .startObject("numeric_dv")
                .field("type", "integer")
                .field("doc_values", true)
                .endObject()
                .startObject("keyword_dv")
                .field("type", "keyword")
                .field("doc_values", true)
                .endObject()
                .startObject("keyword")
                .field("type", "keyword")
                .field("doc_values", false)
                .endObject()
                .endObject()
                .endObject();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static XContentBuilder createNestedTestMapping() {
        try {
            return jsonBuilder().startObject()
                .startObject("properties")
                .startObject("foo")
                .field("type", "integer")
                .endObject()
                .startObject("foo1")
                .field("type", "keyword")
                .endObject()
                .startObject("contacts")
                .field("type", "nested")
                .startObject("properties")
                .startObject("name")
                .field("type", "keyword")
                .endObject()
                .startObject("age")
                .field("type", "integer")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject();
        } catch (IOException e) {
            throw new IllegalStateException(e);
        }
    }

    private static void addNestedDocuments(String id, int foo, String foo1, String name, int age) throws IOException {
        XContentBuilder sourceBuilder = jsonBuilder().startObject()
            .field("foo", foo)
            .field("foo1", foo1)
            .startArray("contacts")
            .startObject()
            .field("name", name)
            .field("age", age)
            .endObject()
            .endArray()
            .endObject();

        client().prepareIndex("nested-test-index").setId(id).setSource(sourceBuilder).get();
    }

    public void testIndexSort() {
        SortField dateSort = new SortedNumericSortField("date", SortField.Type.LONG, false);
        dateSort.setMissingValue(Long.MAX_VALUE);
        SortField numericSort = new SortedNumericSortField("numeric_dv", SortField.Type.INT, false);
        numericSort.setMissingValue(Integer.MAX_VALUE);
        SortField keywordSort = new SortedSetSortField("keyword_dv", false);
        keywordSort.setMissingValue(SortField.STRING_LAST);
        Sort indexSort = new Sort(dateSort, numericSort, keywordSort);
        prepareCreate("test").setSettings(
            Settings.builder()
                .put(indexSettings())
                .put("index.number_of_shards", "1")
                .put("index.number_of_replicas", "1")
                .putList("index.sort.field", "date", "numeric_dv", "keyword_dv")
        ).setMapping(TEST_MAPPING).get();
        for (int i = 0; i < 20; i++) {
            client().prepareIndex("test")
                .setId(Integer.toString(i))
                .setSource("numeric_dv", randomInt(), "keyword_dv", randomAlphaOfLengthBetween(10, 20))
                .get();
        }
        flushAndRefresh();
        ensureYellow();
        assertSortedSegments("test", indexSort);
    }

    public void testInvalidIndexSort() {
        IllegalArgumentException exc = expectThrows(
            IllegalArgumentException.class,
            () -> prepareCreate("test").setSettings(Settings.builder().put(indexSettings()).putList("index.sort.field", "invalid_field"))
                .setMapping(TEST_MAPPING)
                .get()
        );
        assertThat(exc.getMessage(), containsString("unknown index sort field:[invalid_field]"));

        exc = expectThrows(
            IllegalArgumentException.class,
            () -> prepareCreate("test").setSettings(Settings.builder().put(indexSettings()).putList("index.sort.field", "numeric"))
                .setMapping(TEST_MAPPING)
                .get()
        );
        assertThat(exc.getMessage(), containsString("docvalues not found for index sort field:[numeric]"));

        exc = expectThrows(
            IllegalArgumentException.class,
            () -> prepareCreate("test").setSettings(Settings.builder().put(indexSettings()).putList("index.sort.field", "keyword"))
                .setMapping(TEST_MAPPING)
                .get()
        );
        assertThat(exc.getMessage(), containsString("docvalues not found for index sort field:[keyword]"));
    }

    public void testIndexSortOnNestedField() throws IOException {
        SortField regularSort = new SortedNumericSortField("foo", SortField.Type.INT, false);
        regularSort.setMissingValue(Integer.MAX_VALUE);

        Sort indexSort = new Sort(regularSort);

        prepareCreate("nested-test-index").setSettings(
            Settings.builder()
                .put(indexSettings())
                .put("index.number_of_shards", "1")
                .put("index.number_of_replicas", "0")
                .putList("index.sort.field", "foo")
                .putList("index.sort.order", "asc")
        ).setMapping(NESTED_TEST_MAPPING).get();

        addNestedDocuments("1", 30, "", "Alice", 30);
        addNestedDocuments("2", 40, "", "Charlie", 40);
        addNestedDocuments("3", 20, "", "Divs", 20);

        flushAndRefresh("nested-test-index");
        ensureGreen("nested-test-index");

        assertSortedSegments("nested-test-index", indexSort);

        SearchResponse response = client().prepareSearch("nested-test-index")
            .addSort("foo", SortOrder.ASC)
            .setQuery(QueryBuilders.matchAllQuery())
            .get();

        assertEquals(3, response.getHits().getTotalHits().value());
        assertEquals("3", response.getHits().getAt(0).getId());
        assertEquals("1", response.getHits().getAt(1).getId());
        assertEquals("2", response.getHits().getAt(2).getId());
    }

    public void testIndexSortWithNestedField_MultiField() throws IOException {
        prepareCreate("nested-test-index").setSettings(
            Settings.builder()
                .put(indexSettings())
                .put("index.number_of_shards", "1")
                .put("index.number_of_replicas", "0")
                .putList("index.sort.field", "foo", "foo1")
                .putList("index.sort.order", "desc", "asc")
        ).setMapping(NESTED_TEST_MAPPING).get();

        addNestedDocuments("1", 40, "Charlie", "Charlie", 40);
        addNestedDocuments("2", 30, "Divs", "Divs", 30);
        addNestedDocuments("3", 30, "Alice", "Alice", 30);

        flushAndRefresh("nested-test-index");
        ensureGreen("nested-test-index");
        SearchResponse response = client().prepareSearch("nested-test-index")
            .addSort("foo", SortOrder.DESC)
            .addSort("foo1", SortOrder.ASC)
            .setQuery(QueryBuilders.matchAllQuery())
            .get();

        assertEquals(3, response.getHits().getTotalHits().value());
        assertEquals(40, response.getHits().getAt(0).getSourceAsMap().get("foo"));
        assertEquals("Charlie", response.getHits().getAt(0).getSourceAsMap().get("foo1"));

        assertEquals(30, response.getHits().getAt(1).getSourceAsMap().get("foo"));
        assertEquals(30, response.getHits().getAt(2).getSourceAsMap().get("foo"));

        // specifically verify secondary sort on foo1 when foo values are the same
        // since foo1 sort is asc, "ALice" should come before "Divs"
        assertEquals("Alice", response.getHits().getAt(1).getSourceAsMap().get("foo1"));
        assertEquals("Divs", response.getHits().getAt(2).getSourceAsMap().get("foo1"));

    }

    public void testIndexSortWithSortFieldInsideDocBlock() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> prepareCreate("nested-sort-test").setSettings(
                Settings.builder()
                    .put(indexSettings())
                    .put("index.number_of_shards", "1")
                    .put("index.number_of_replicas", "0")
                    .putList("index.sort.field", "contacts.age")
                    .putList("index.sort.order", "desc")
            ).setMapping(NESTED_TEST_MAPPING).get()
        );

        assertThat(exception.getMessage(), containsString("index sorting on nested fields is not supported"));
    }
}
