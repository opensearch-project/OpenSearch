/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl;

import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.search.sort.SortMode;
import org.opensearch.search.sort.SortOrder;

/**
 * Integration tests for DSL sort and pagination conversion.
 * Uses matchAllQuery; focus is on sort/from/size behavior.
 */
public class DslSortIT extends DslIntegTestBase {

    public void testDefaultPagination() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder()));
    }

    public void testSortAscending() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().sort("name", SortOrder.ASC)));
    }

    public void testSortDescending() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().sort("price", SortOrder.DESC)));
    }

    public void testMultipleSortFields() {
        createTestIndex();
        assertOk(search(
            new SearchSourceBuilder()
                .sort("brand", SortOrder.ASC)
                .sort("price", SortOrder.DESC)
        ));
    }

    public void testCustomSize() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().size(5)));
    }

    public void testFromAndSize() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().from(0).size(5)));
    }

    public void testFromOffset() {
        createTestIndex();
        assertOk(search(new SearchSourceBuilder().from(10).size(5)));
    }

    public void testSortModeMin() {
        createTestIndexWithArrayField();
        FieldSortBuilder sortBuilder = new FieldSortBuilder("tags").order(SortOrder.ASC).sortMode(SortMode.MIN);
        assertOk(search(new SearchSourceBuilder().sort(sortBuilder)));
    }

    public void testSortModeMax() {
        createTestIndexWithArrayField();
        FieldSortBuilder sortBuilder = new FieldSortBuilder("tags").order(SortOrder.DESC).sortMode(SortMode.MAX);
        assertOk(search(new SearchSourceBuilder().sort(sortBuilder)));
    }

    public void testSortModeAvg() {
        createTestIndexWithArrayField();
        FieldSortBuilder sortBuilder = new FieldSortBuilder("tags").order(SortOrder.ASC).sortMode(SortMode.AVG);
        assertOk(search(new SearchSourceBuilder().sort(sortBuilder)));
    }

    public void testSortModeSum() {
        createTestIndexWithArrayField();
        FieldSortBuilder sortBuilder = new FieldSortBuilder("tags").order(SortOrder.ASC).sortMode(SortMode.SUM);
        assertOk(search(new SearchSourceBuilder().sort(sortBuilder)));
    }

    public void testSortModeMedian() {
        createTestIndexWithArrayField();
        FieldSortBuilder sortBuilder = new FieldSortBuilder("tags").order(SortOrder.ASC).sortMode(SortMode.MEDIAN);
        // MEDIAN is not yet supported, expect failure
        expectThrows(Exception.class, () -> search(new SearchSourceBuilder().sort(sortBuilder)));
    }

    public void testSortModeWithMultipleSorts() {
        createTestIndexWithArrayField();
        FieldSortBuilder modeSort = new FieldSortBuilder("tags").order(SortOrder.ASC).sortMode(SortMode.MIN);
        assertOk(search(
            new SearchSourceBuilder()
                .sort(modeSort)
                .sort("name", SortOrder.DESC)
        ));
    }

    public void testSortModeWithPagination() {
        createTestIndexWithArrayField();
        FieldSortBuilder sortBuilder = new FieldSortBuilder("tags").order(SortOrder.ASC).sortMode(SortMode.AVG);
        assertOk(search(new SearchSourceBuilder().sort(sortBuilder).from(0).size(5)));
    }
}
