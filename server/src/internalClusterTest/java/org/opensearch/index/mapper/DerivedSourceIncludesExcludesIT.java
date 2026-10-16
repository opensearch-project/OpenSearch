/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.action.get.GetResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Locale;
import java.util.Map;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * Integration tests for _source includes/excludes functionality with derived source.
 * These tests verify that _source filtering (includes/excludes) works correctly
 * when derived source is enabled, both when reading from translog and from segments.
 */
public class DerivedSourceIncludesExcludesIT extends OpenSearchIntegTestCase {

    private static final String MAPPING_TEMPLATE = "{\n"
        + "  \"_source\": { %s },\n"
        + "  \"properties\": {\n"
        + "    \"name\": { \"type\": \"keyword\" },\n"
        + "    \"age\": { \"type\": \"integer\" },\n"
        + "    \"city\": { \"type\": \"keyword\" }\n"
        + "  }\n"
        + "}";

    private void createIndex(String indexName, boolean derivedSourceEnabled, String sourceFilter) {
        Settings.Builder settings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.refresh_interval", -1); // Disable background refresh to ensure translog reads
        if (derivedSourceEnabled) {
            settings.put("index.derived_source.enabled", true);
        }
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(indexName)
                .setSettings(settings)
                .setMapping(String.format(Locale.ROOT, MAPPING_TEMPLATE, sourceFilter))
        );
        ensureGreen(indexName);
    }

    private void indexDocument(String indexName, String id, boolean refresh) {
        client().prepareIndex(indexName)
            .setId(id)
            .setSource("{\"name\":\"test\",\"age\":25,\"city\":\"seattle\"}", XContentType.JSON)
            .get();
        if (refresh) {
            refresh(indexName);
        }
    }

    private Map<String, Object> getSourceAsMap(String indexName, String id) {
        GetResponse getResponse = client().prepareGet(indexName, id).get();
        return getResponse.getSourceAsMap();
    }

    private void assertExcludesAge(boolean derivedSourceEnabled, boolean refresh) {
        String indexName = "test-excludes-" + derivedSourceEnabled + "-" + refresh;
        createIndex(indexName, derivedSourceEnabled, "\"excludes\": [\"age\"]");
        indexDocument(indexName, "1", refresh);

        Map<String, Object> source = getSourceAsMap(indexName, "1");

        assertFalse(
            "age should be excluded (derived_source=" + derivedSourceEnabled + ", refresh=" + refresh + ")",
            source.containsKey("age")
        );
        assertTrue("name should exist (derived_source=" + derivedSourceEnabled + ", refresh=" + refresh + ")", source.containsKey("name"));
        assertTrue("city should exist (derived_source=" + derivedSourceEnabled + ", refresh=" + refresh + ")", source.containsKey("city"));
    }

    private void assertIncludesOnlyName(boolean derivedSourceEnabled, boolean refresh) {
        String indexName = "test-includes-" + derivedSourceEnabled + "-" + refresh;
        createIndex(indexName, derivedSourceEnabled, "\"includes\": [\"name\"]");
        indexDocument(indexName, "1", refresh);

        Map<String, Object> source = getSourceAsMap(indexName, "1");

        assertTrue("name should exist (derived_source=" + derivedSourceEnabled + ", refresh=" + refresh + ")", source.containsKey("name"));
        assertFalse(
            "age should not appear (derived_source=" + derivedSourceEnabled + ", refresh=" + refresh + ")",
            source.containsKey("age")
        );
        assertFalse(
            "city should not appear (derived_source=" + derivedSourceEnabled + ", refresh=" + refresh + ")",
            source.containsKey("city")
        );
    }

    public void testExcludesAge_Translog() {
        assertExcludesAge(true, false);
    }

    public void testExcludesAge_Segments() {
        assertExcludesAge(true, true);
    }

    public void testExcludesAge_SegmentsWithoutDerivedSource() {
        assertExcludesAge(false, true);
    }

    public void testIncludesOnlyName_Translog() {
        assertIncludesOnlyName(true, false);
    }

    public void testIncludesOnlyName_Segments() {
        assertIncludesOnlyName(true, true);
    }

    public void testIncludesOnlyName_SegmentsWithoutDerivedSource() {
        assertIncludesOnlyName(false, true);
    }
}
