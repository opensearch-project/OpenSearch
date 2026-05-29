/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.mapper.MapperParsingException;
import org.opensearch.test.OpenSearchIntegTestCase;

/**
 * Integration tests validating field capability assignment for composite indices.
 * Verifies that supported field types create successfully and unsupported types
 * are rejected with appropriate errors.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class CompositeFieldCapabilityIT extends AbstractCompositeEngineIT {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).build();
    }

    private Settings dfaSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .build();
    }

    private void startCluster() {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
    }

    private void assertIndexCreationSucceeds(String indexName, String fieldName, String mappingType) {
        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(dfaSettings())
            .setMapping(fieldName, mappingType)
            .get();
        assertTrue(response.isAcknowledged());
        ensureGreen(indexName);
    }

    private void assertIndexCreationFails(String indexName, String fieldName, String mappingType) {
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> client().admin().indices().prepareCreate(indexName).setSettings(dfaSettings()).setMapping(fieldName, mappingType).get()
        );
    }

    // === SUPPORTED FIELDS (expect 200) ===

    public void testLongFieldSupported() {
        startCluster();
        assertIndexCreationSucceeds("test-long", "field", "type=long");
    }

    public void testIntegerFieldSupported() {
        startCluster();
        assertIndexCreationSucceeds("test-integer", "field", "type=integer");
    }

    public void testShortFieldSupported() {
        startCluster();
        assertIndexCreationSucceeds("test-short", "field", "type=short");
    }

    public void testByteFieldSupported() {
        startCluster();
        assertIndexCreationSucceeds("test-byte", "field", "type=byte");
    }

    public void testDoubleFieldSupported() {
        startCluster();
        assertIndexCreationSucceeds("test-double", "field", "type=double");
    }

    public void testFloatFieldSupported() {
        startCluster();
        assertIndexCreationSucceeds("test-float", "field", "type=float");
    }

    public void testHalfFloatFieldSupported() {
        startCluster();
        assertIndexCreationSucceeds("test-half-float", "field", "type=half_float");
    }

    public void testUnsignedLongFieldSupported() {
        startCluster();
        assertIndexCreationSucceeds("test-unsigned-long", "field", "type=unsigned_long");
    }

    // scaled_float requires the mapper-extras module which is not a dependency of this plugin.
    // The searchCapability override is tested in modules/mapper-extras ScaledFloatFieldMapperTests.

    public void testDateFieldSupported() {
        startCluster();
        assertIndexCreationSucceeds("test-date", "field", "type=date");
    }

    public void testDateNanosFieldSupported() {
        startCluster();
        assertIndexCreationSucceeds("test-date-nanos", "field", "type=date_nanos");
    }

    public void testBooleanFieldSupported() {
        startCluster();
        assertIndexCreationSucceeds("test-boolean", "field", "type=boolean,index=false");
    }

    public void testKeywordFieldSupported() {
        startCluster();
        assertIndexCreationSucceeds("test-keyword", "field", "type=keyword");
    }

    public void testTextFieldSupported() {
        startCluster();
        assertIndexCreationSucceeds("test-text", "field", "type=text");
    }

    public void testIpFieldSupported() {
        startCluster();
        assertIndexCreationSucceeds("test-ip", "field", "type=ip");
    }

    public void testBinaryFieldSupported() {
        startCluster();
        assertIndexCreationSucceeds("test-binary", "field", "type=binary,doc_values=true");
    }

    public void testMatchOnlyTextFieldSupported() {
        startCluster();
        assertIndexCreationSucceeds("test-match-only-text", "field", "type=match_only_text");
    }

    // token_count requires the mapper-extras module which is not a dependency of this plugin.

    // === UNSUPPORTED FIELDS (expect 400 MapperParsingException) ===

    public void testGeoPointFieldUnsupported() {
        startCluster();
        assertIndexCreationFails("test-geo-point", "field", "type=geo_point");
    }

    public void testGeoShapeFieldUnsupported() {
        startCluster();
        assertIndexCreationFails("test-geo-shape", "field", "type=geo_shape");
    }

    public void testCompletionFieldUnsupported() {
        startCluster();
        assertIndexCreationFails("test-completion", "field", "type=completion");
    }

    public void testNestedFieldUnsupported() {
        startCluster();
        MapperParsingException ex = expectThrows(
            MapperParsingException.class,
            () -> client().admin()
                .indices()
                .prepareCreate("test-nested")
                .setSettings(dfaSettings())
                .setMapping("field", "type=nested")
                .get()
        );
        assertTrue(ex.getMessage().contains("nested type is not supported with pluggable data format"));
    }

    public void testFlatObjectFieldUnsupported() {
        startCluster();
        assertIndexCreationFails("test-flat-object", "field", "type=flat_object");
    }

    public void testWildcardFieldUnsupported() {
        startCluster();
        assertIndexCreationFails("test-wildcard", "field", "type=wildcard");
    }

    // === SPECIAL CASES ===

    public void testMultiFieldTextWithKeywordSubfield() throws Exception {
        startCluster();
        String mapping = "{\n"
            + "  \"properties\": {\n"
            + "    \"field\": {\n"
            + "      \"type\": \"text\",\n"
            + "      \"fields\": {\n"
            + "        \"raw\": { \"type\": \"keyword\" }\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}";
        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate("test-multi-field")
            .setSettings(dfaSettings())
            .setMapping(mapping)
            .get();
        assertTrue(response.isAcknowledged());
        ensureGreen("test-multi-field");
    }

    public void testFieldWithOnlyDocValues() {
        startCluster();
        assertIndexCreationSucceeds("test-doc-values-only", "field", "type=keyword,index=false,doc_values=true");
    }

    public void testFieldWithOnlyStored() {
        startCluster();
        assertIndexCreationSucceeds("test-stored-only", "field", "type=keyword,index=false,doc_values=false,store=true");
    }

    public void testFieldWithAllDisabled() {
        startCluster();
        assertIndexCreationSucceeds("test-all-disabled", "field", "type=keyword,index=false,doc_values=false,store=false");
    }

    public void testMultipleFieldsAllSupported() {
        startCluster();
        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate("test-multiple-fields")
            .setSettings(dfaSettings())
            .setMapping("f_long", "type=long", "f_keyword", "type=keyword", "f_date", "type=date", "f_text", "type=text")
            .get();
        assertTrue(response.isAcknowledged());
        ensureGreen("test-multiple-fields");
    }

    public void testDynamicMappingNumericField() {
        startCluster();
        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate("test-dynamic-mapping")
            .setSettings(dfaSettings())
            .setMapping("name", "type=keyword")
            .get();
        assertTrue(response.isAcknowledged());
        ensureGreen("test-dynamic-mapping");

        assertEquals(
            RestStatus.CREATED,
            client().prepareIndex().setIndex("test-dynamic-mapping").setId("1").setSource("name", "test", "dynamic_num", 42).get().status()
        );
    }
}
