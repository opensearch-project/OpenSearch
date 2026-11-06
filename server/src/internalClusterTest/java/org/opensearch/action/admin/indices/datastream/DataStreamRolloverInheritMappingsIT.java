/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.datastream;

import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.admin.indices.mapping.get.GetMappingsResponse;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.cluster.metadata.DataStream;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;

import java.util.Map;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.not;

/**
 * Verifies that when {@link IndexMetadata#INDEX_DATA_STREAM_ROLLOVER_INHERIT_MAPPINGS_SETTING} is enabled on the matching
 * composable index template, a data stream rollover creates the new backing index with exactly the mappings of the
 * previous write index, and that the default behaviour (mappings resolved from the template) is otherwise unchanged.
 */
public class DataStreamRolloverInheritMappingsIT extends DataStreamTestCase {

    private static final String TEMPLATE = "logs-template";
    private static final String STREAM = "logs-demo";
    private static final String GEN_1 = DataStream.getDefaultBackingIndexName(STREAM, 1);
    private static final String GEN_2 = DataStream.getDefaultBackingIndexName(STREAM, 2);

    private static final String BASE_MAPPING = "{\"properties\":{\"@timestamp\":{\"type\":\"date\"},\"host\":{\"type\":\"keyword\"}}}";
    private static final String UPDATED_MAPPING = "{\"properties\":{\"@timestamp\":{\"type\":\"date\"},\"host\":{\"type\":\"keyword\"},"
        + "\"template_only\":{\"type\":\"long\"}}}";

    public void testRolloverUsesTemplateMappingsByDefault() throws Exception {
        putTemplate(BASE_MAPPING, null);
        createDataStream(STREAM);
        indexDocWithDynamicField();
        assertThat(properties(GEN_1), hasKey("dynamic_field"));

        rolloverDataStream(STREAM);

        // Today's behaviour: the new backing index only gets what the template defines.
        assertThat(properties(GEN_2), not(hasKey("dynamic_field")));
        assertThat(properties(GEN_2), hasKey("host"));
    }

    public void testRolloverInheritsMappingsWhenEnabledOnTemplate() throws Exception {
        putTemplate(BASE_MAPPING, true);
        createDataStream(STREAM);
        indexDocWithDynamicField();

        rolloverDataStream(STREAM);

        assertThat(properties(GEN_2), hasKey("dynamic_field"));
        assertThat(mapping(GEN_2), equalTo(mapping(GEN_1)));
    }

    public void testRolloverCarriesForwardExplicitMappingUpdates() throws Exception {
        putTemplate(BASE_MAPPING, true);
        createDataStream(STREAM);
        PutMappingRequest putMapping = new PutMappingRequest(STREAM).source(
            "{\"properties\":{\"explicit_field\":{\"type\":\"integer\"}}}",
            XContentType.JSON
        );
        assertAcked(client().admin().indices().putMapping(putMapping).get());

        rolloverDataStream(STREAM);

        assertThat(properties(GEN_2), hasKey("explicit_field"));
        assertThat(mapping(GEN_2), equalTo(mapping(GEN_1)));
    }

    public void testTemplateMappingChangesAreIgnoredWhenEnabled() throws Exception {
        putTemplate(BASE_MAPPING, true);
        createDataStream(STREAM);
        // Template mappings change after the write index was created; strict copy must not pick them up.
        putTemplate(UPDATED_MAPPING, true);

        rolloverDataStream(STREAM);

        assertThat(properties(GEN_2), not(hasKey("template_only")));
        assertThat(mapping(GEN_2), equalTo(mapping(GEN_1)));
    }

    public void testEnablingOnTemplateTakesEffectOnNextRollover() throws Exception {
        // Write index is created while the template does not carry the flag.
        putTemplate(BASE_MAPPING, null);
        createDataStream(STREAM);
        indexDocWithDynamicField();
        assertFalse(IndexMetadata.INDEX_DATA_STREAM_ROLLOVER_INHERIT_MAPPINGS_SETTING.exists(indexSettings(GEN_1)));

        // Flag is enabled on the template only; the very next rollover must inherit, without a one-generation lag.
        putTemplate(BASE_MAPPING, true);
        rolloverDataStream(STREAM);

        assertThat(properties(GEN_2), hasKey("dynamic_field"));
        assertThat(mapping(GEN_2), equalTo(mapping(GEN_1)));
    }

    public void testDisablingOnTemplateTakesEffectOnNextRollover() throws Exception {
        // Write index is created from a template that enables the flag, so the write index carries it as well.
        putTemplate(BASE_MAPPING, true);
        createDataStream(STREAM);
        indexDocWithDynamicField();
        assertTrue(IndexMetadata.INDEX_DATA_STREAM_ROLLOVER_INHERIT_MAPPINGS_SETTING.get(indexSettings(GEN_1)));

        // Flag is disabled on the template; the template is the only source of truth, so the write index value is ignored.
        putTemplate(BASE_MAPPING, false);
        rolloverDataStream(STREAM);

        assertThat(properties(GEN_2), not(hasKey("dynamic_field")));
    }

    private void putTemplate(String mapping, Boolean inheritMappings) throws Exception {
        String flag = inheritMappings == null
            ? ""
            : ",\"" + IndexMetadata.SETTING_DATA_STREAM_ROLLOVER_INHERIT_MAPPINGS + "\":" + inheritMappings;
        String json = "{\"index_patterns\":[\"logs-*\"],\"data_stream\":{},\"template\":{"
            + "\"settings\":{\"number_of_shards\":1,\"number_of_replicas\":0"
            + flag
            + "},\"mappings\":"
            + mapping
            + "}}";
        createIndexTemplate(TEMPLATE, json);
    }

    private void indexDocWithDynamicField() {
        client().prepareIndex(STREAM)
            .setOpType(DocWriteRequest.OpType.CREATE)
            .setSource("{\"@timestamp\":\"2026-01-01T00:00:00Z\",\"host\":\"h1\",\"dynamic_field\":\"v\"}", XContentType.JSON)
            .get();
    }

    private Map<String, Object> mapping(String index) {
        GetMappingsResponse response = client().admin().indices().prepareGetMappings(index).get();
        return response.getMappings().get(index).sourceAsMap();
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> properties(String index) {
        return (Map<String, Object>) mapping(index).get("properties");
    }

    private Settings indexSettings(String index) {
        return client().admin().indices().prepareGetSettings(index).get().getIndexToSettings().get(index);
    }
}
