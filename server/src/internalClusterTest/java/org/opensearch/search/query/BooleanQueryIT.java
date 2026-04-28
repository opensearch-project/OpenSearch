/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.query;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.common.settings.Settings;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import static org.opensearch.index.query.QueryBuilders.boolQuery;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.index.query.QueryBuilders.matchQuery;
import static org.opensearch.index.query.QueryBuilders.rangeQuery;
import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.opensearch.index.query.QueryBuilders.termsQuery;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

public class BooleanQueryIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    public BooleanQueryIT(Settings staticSettings) {
        super(staticSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    public void testMustNotRangeRewrite() throws Exception {
        String intField = "int_field";
        String termField1 = "term_field_1";
        String termField2 = "term_field_2";
        // If we don't explicitly set this mapping, term_field_2 is not correctly mapped as a keyword field and queries don't work as
        // expected
        createIndex(
            "test",
            Settings.EMPTY,
            "{\"properties\":{\"int_field\":{\"type\": \"integer\"},\"term_field_1\":{\"type\": \"keyword\"},\"term_field_2\":{\"type\": \"keyword\"}}}}"
        );
        int numDocs = 100;

        for (int i = 0; i < numDocs; i++) {
            String termValue1 = "odd";
            String termValue2 = "A";
            if (i % 2 == 0) {
                termValue1 = "even";
            }
            if (i >= numDocs / 2) {
                termValue2 = "B";
            }
            client().prepareIndex("test")
                .setId(Integer.toString(i))
                .setSource(intField, i, termField1, termValue1, termField2, termValue2)
                .get();
        }
        ensureGreen();
        waitForRelocation();
        forceMerge();
        refresh();

        int lt = 80;
        int gte = 20;
        int expectedHitCount = numDocs - (lt - gte);

        assertHitCount(
            client().prepareSearch().setQuery(boolQuery().mustNot(rangeQuery(intField).lt(lt).gte(gte))).get(),
            expectedHitCount
        );

        assertHitCount(
            client().prepareSearch()
                .setQuery(boolQuery().must(termQuery(termField1, "even")).mustNot(rangeQuery(intField).lt(lt).gte(gte)))
                .get(),
            expectedHitCount / 2
        );

        // Test with preexisting should fields, to ensure the original default minimumShouldMatch is respected
        // once we add a must clause during rewrite, even if minimumShouldMatch is not explicitly set
        assertHitCount(
            client().prepareSearch().setQuery(boolQuery().should(termQuery(termField1, "even")).should(termQuery(termField2, "B"))).get(),
            3 * numDocs / 4
        );
        assertHitCount(
            client().prepareSearch()
                .setQuery(
                    boolQuery().should(termQuery(termField1, "even"))
                        .should(termQuery(termField2, "A"))
                        .mustNot(rangeQuery(intField).lt(lt).gte(gte))
                )
                .get(),
            3 * expectedHitCount / 4
        );

        assertHitCount(client().prepareSearch().setQuery(boolQuery().mustNot(rangeQuery(intField).lt(numDocs * 2))).get(), 0);
    }

    public void testMustNotDateRangeRewrite() throws Exception {
        int minDay = 10;
        int maxDay = 31;
        String dateField = "date_field";
        createIndex("test");
        for (int day = minDay; day <= maxDay; day++) {
            client().prepareIndex("test").setSource(dateField, getDate(day, 0)).get();
        }
        ensureGreen();
        waitForRelocation();
        forceMerge();
        refresh();

        int minExcludedDay = 15;
        int maxExcludedDay = 25;
        int expectedHitCount = maxDay + 1 - minDay - (maxExcludedDay - minExcludedDay + 1);

        assertHitCount(
            client().prepareSearch("test")
                .setQuery(boolQuery().mustNot(rangeQuery(dateField).gte(getDate(minExcludedDay, 0)).lte(getDate(maxExcludedDay, 0))))
                .get(),
            expectedHitCount
        );
    }

    public void testMustNotDateRangeWithFormatAndTimeZoneRewrite() throws Exception {
        // Index a document for each hour of 1/1 in UTC. Then, do a boolean query excluding 1/1 for UTC+3.
        // If the time zone is respected by the rewrite, we should see 3 matching documents,
        // as there will be 3 hours that are 1/1 in UTC but not UTC+3.

        String dateField = "date_field";
        createIndex("test");
        String format = "strict_date_hour_minute_second_fraction";

        for (int hour = 0; hour < 24; hour++) {
            client().prepareIndex("test").setSource(dateField, getDate(1, hour)).get();
        }
        ensureGreen();
        waitForRelocation();
        forceMerge();
        refresh();

        int zoneOffset = 3;
        assertHitCount(
            client().prepareSearch("test")
                .setQuery(
                    boolQuery().mustNot(
                        rangeQuery(dateField).gte(getDate(1, 0)).lte(getDate(1, 23)).timeZone("+" + zoneOffset).format(format)
                    )
                )
                .get(),
            zoneOffset
        );
    }

    public void testMustNotRangeRewriteWithMissingValues() throws Exception {
        // This test passes because the rewrite is skipped when not all docs have exactly 1 value
        String intField = "int_field";
        String termField = "term_field";
        createIndex("test");
        int numDocs = 100;
        int numDocsMissingIntValues = 20;

        for (int i = 0; i < numDocs; i++) {
            String termValue = "odd";
            if (i % 2 == 0) {
                termValue = "even";
            }

            if (i >= numDocsMissingIntValues) {
                client().prepareIndex("test").setId(Integer.toString(i)).setSource(intField, i, termField, termValue).get();
            } else {
                client().prepareIndex("test").setId(Integer.toString(i)).setSource(termField, termValue).get();
            }
        }
        ensureGreen();
        waitForRelocation();
        forceMerge();
        refresh();

        // Search excluding range 30 to 50

        int lt = 50;
        int gte = 30;
        int expectedHitCount = numDocs - (lt - gte); // Expected hit count includes documents missing this value

        assertHitCount(
            client().prepareSearch().setQuery(boolQuery().mustNot(rangeQuery(intField).lt(lt).gte(gte))).get(),
            expectedHitCount
        );
    }

    public void testMustNotRangeRewriteWithMoreThanOneValue() throws Exception {
        // This test passes because the rewrite is skipped when not all docs have exactly 1 value
        String intField = "int_field";
        createIndex("test");
        int numDocs = 100;
        int numDocsWithExtraValue = 20;
        int extraValue = -1;

        for (int i = 0; i < numDocs; i++) {
            if (i < numDocsWithExtraValue) {
                client().prepareIndex("test").setId(Integer.toString(i)).setSource(intField, new int[] { extraValue, i }).get();
            } else {
                client().prepareIndex("test").setId(Integer.toString(i)).setSource(intField, i).get();
            }
        }
        ensureGreen();
        waitForRelocation();
        forceMerge();
        refresh();

        // Range queries will match if ANY of the doc's values are within the range.
        // So if we exclude the range 0 to 20, we shouldn't see any of those documents returned,
        // even though they also have a value -1 which is not excluded.

        int expectedHitCount = numDocs - numDocsWithExtraValue;
        assertHitCount(
            client().prepareSearch().setQuery(boolQuery().mustNot(rangeQuery(intField).lt(numDocsWithExtraValue).gte(0))).get(),
            expectedHitCount
        );
        assertHitCount(client().prepareSearch().setQuery(matchAllQuery()).get(), numDocs);
    }

    public void testMustNotNumericMatchOrTermQueryRewrite() throws Exception {
        Map<Integer, Integer> statusToDocCountMap = Map.of(200, 1000, 404, 30, 500, 1, 400, 1293);
        String statusField = "status";
        createIndex("test");
        int totalDocs = 0;
        for (Map.Entry<Integer, Integer> entry : statusToDocCountMap.entrySet()) {
            for (int i = 0; i < entry.getValue(); i++) {
                client().prepareIndex("test").setSource(statusField, entry.getKey()).get();
            }
            totalDocs += entry.getValue();
        }
        ensureGreen();
        waitForRelocation();
        forceMerge();
        refresh();

        int excludedValue = randomFrom(statusToDocCountMap.keySet());
        int expectedHitCount = totalDocs - statusToDocCountMap.get(excludedValue);

        // Check the rewritten match query behaves as expected
        assertHitCount(
            client().prepareSearch("test").setQuery(boolQuery().mustNot(matchQuery(statusField, excludedValue))).get(),
            expectedHitCount
        );

        // Check the rewritten term query behaves as expected
        assertHitCount(
            client().prepareSearch("test").setQuery(boolQuery().mustNot(termQuery(statusField, excludedValue))).get(),
            expectedHitCount
        );

        // Check a rewritten terms query behaves as expected
        List<Integer> excludedValues = new ArrayList<>();
        excludedValues.add(excludedValue);
        int secondExcludedValue = randomFrom(statusToDocCountMap.keySet());
        expectedHitCount = totalDocs - statusToDocCountMap.get(excludedValue);
        if (secondExcludedValue != excludedValue) {
            excludedValues.add(secondExcludedValue);
            expectedHitCount -= statusToDocCountMap.get(secondExcludedValue);
        }

        assertHitCount(
            client().prepareSearch("test").setQuery(boolQuery().mustNot(termsQuery(statusField, excludedValues))).get(),
            expectedHitCount
        );
    }

    public void testMustToFilterRewrite() throws Exception {
        // Check we still get expected behavior after rewriting must clauses --> filter clauses.
        String intField = "int_field";
        createIndex("test");
        int numDocs = 100;

        for (int i = 0; i < numDocs; i++) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource(intField, i).get();
        }
        ensureGreen();
        waitForRelocation();
        forceMerge();
        refresh();

        int gt = 22;
        int lt = 92;
        int expectedHitCount = lt - gt - 1;
        assertHitCount(client().prepareSearch().setQuery(boolQuery().must(rangeQuery(intField).lt(lt).gt(gt))).get(), expectedHitCount);
    }

    private String padZeros(int value, int length) {
        // String.format() not allowed
        String ret = Integer.toString(value);
        if (ret.length() < length) {
            ret = "0".repeat(length - ret.length()) + ret;
        }
        return ret;
    }

    private String getDate(int day, int hour) {
        return "2016-01-" + padZeros(day, 2) + "T" + padZeros(hour, 2) + ":00:00.000";
    }
}
