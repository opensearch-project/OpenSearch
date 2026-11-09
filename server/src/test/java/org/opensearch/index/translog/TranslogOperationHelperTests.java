/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.mapper.DocumentMapperForType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.MapperServiceTestCase;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.test.IndexSettingsModule;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class TranslogOperationHelperTests extends MapperServiceTestCase {

    private EngineConfig engineConfig;
    private TranslogOperationHelper helper;
    private TranslogOperationHelper helperWithDerivedSource;
    private MapperService mapperService;
    private DocumentMapperForType documentMapperForType;

    @Before
    public void setup() throws Exception {
        // Setup for normal case
        final IndexMetadata defaultIndexMetadata = IndexMetadata.builder("test")
            .settings(settings(Version.CURRENT))
            .numberOfShards(1)
            .numberOfReplicas(1)
            .build();
        IndexSettings defaultIndexSettings = IndexSettingsModule.newIndexSettings("test", defaultIndexMetadata.getSettings());

        engineConfig = createEngineConfig(defaultIndexSettings);
        helper = TranslogOperationHelper.create(engineConfig);

        // Setup for derived source case
        Settings derivedSourceSettings = Settings.builder()
            .put(defaultIndexSettings.getSettings())
            .put("index.derived_source.enabled", true)
            .build();
        IndexMetadata derivedMetadata = IndexMetadata.builder("test").settings(derivedSourceSettings).build();
        IndexSettings derivedIndexSettings = new IndexSettings(derivedMetadata, Settings.EMPTY);

        mapperService = createMapperService();
        documentMapperForType = mock(DocumentMapperForType.class);
        when(documentMapperForType.getDocumentMapper()).thenReturn(mapperService.documentMapper());

        EngineConfig derivedConfig = createEngineConfig(derivedIndexSettings);
        helperWithDerivedSource = TranslogOperationHelper.create(derivedConfig);
    }

    private MapperService createMapperService() throws IOException {
        return createMapperService(fieldMapping(b -> b.field("type", "keyword")));
    }

    private EngineConfig createEngineConfig(IndexSettings indexSettings) {

        ShardId shardId = new ShardId(indexSettings.getIndex(), 0);

        return new EngineConfig.Builder().shardId(shardId)
            .indexSettings(indexSettings)
            .retentionLeasesSupplier(() -> RetentionLeases.EMPTY)
            .codecService(new CodecService(null, indexSettings, logger, List.of()))
            .documentMapperForTypeSupplier(() -> documentMapperForType)
            .build();
    }

    public void testIdenticalOperationsNormalCase() {
        BytesReference source = new BytesArray("{\"field\":\"value\"}".getBytes(StandardCharsets.UTF_8));
        Translog.Index op1 = new Translog.Index("1", 0, 1, source.toBytesRef().bytes);
        Translog.Index op2 = new Translog.Index("1", 0, 1, source.toBytesRef().bytes);

        assertTrue("Identical operations should be equal", helper.hasSameIndexOperation(op1, op2));
    }

    public void testIdenticalOperationsDerivedSourceCase() {
        BytesReference source = new BytesArray("{\"field\":\"value\"}".getBytes(StandardCharsets.UTF_8));
        Translog.Index op1 = new Translog.Index("1", 0, 1, source.toBytesRef().bytes);
        Translog.Index op2 = new Translog.Index("1", 0, 1, source.toBytesRef().bytes);

        assertTrue("Identical operations should be equal for derived source", helperWithDerivedSource.hasSameIndexOperation(op1, op2));
    }

    public void testDifferentIdsNormalCase() {
        BytesReference source = new BytesArray("{\"field\":\"value\"}".getBytes(StandardCharsets.UTF_8));
        Translog.Index op1 = new Translog.Index("1", 0, 1, source.toBytesRef().bytes);
        Translog.Index op2 = new Translog.Index("2", 0, 1, source.toBytesRef().bytes);

        assertFalse("Operations with different IDs should not be equal", helper.hasSameIndexOperation(op1, op2));
    }

    public void testDifferentSourcesNormalCase() {
        BytesReference source1 = new BytesArray("{\"field\":\"value1\"}".getBytes(StandardCharsets.UTF_8));
        BytesReference source2 = new BytesArray("{\"field\":\"value2\"}".getBytes(StandardCharsets.UTF_8));
        Translog.Index op1 = new Translog.Index("1", 0, 1, source1.toBytesRef().bytes);
        Translog.Index op2 = new Translog.Index("1", 0, 1, source2.toBytesRef().bytes);

        assertFalse("Operations with different sources should not be equal", helper.hasSameIndexOperation(op1, op2));
    }

    public void testDifferentSeqNoNormalCase() {
        BytesReference source = new BytesArray("{\"field\":\"value\"}".getBytes(StandardCharsets.UTF_8));
        Translog.Index op1 = new Translog.Index("1", 0, 1, source.toBytesRef().bytes);
        Translog.Index op2 = new Translog.Index("1", 1, 1, source.toBytesRef().bytes);

        assertFalse("Operations with different sequence numbers should not be equal", helper.hasSameIndexOperation(op1, op2));
    }

    public void testSourceComparisonWithDifferentOrder() {
        BytesReference source1 = new BytesArray("{\"a\":1,\"b\":2}".getBytes(StandardCharsets.UTF_8));
        BytesReference source2 = new BytesArray("{\"b\":2,\"a\":1}".getBytes(StandardCharsets.UTF_8));

        assertTrue("Sources with different field order should be equal", TranslogOperationHelper.compareSourcesWithOrder(source1, source2));
    }

    public void testSourceComparisonWithNestedObjects() {
        BytesReference source1 = new BytesArray("{\"obj\":{\"nested1\":1,\"nested2\":2}}".getBytes(StandardCharsets.UTF_8));
        BytesReference source2 = new BytesArray("{\"obj\":{\"nested2\":2,\"nested1\":1}}".getBytes(StandardCharsets.UTF_8));

        assertTrue(
            "Sources with nested objects in different order should be equal",
            TranslogOperationHelper.compareSourcesWithOrder(source1, source2)
        );
    }

    public void testSourceComparisonWithNestedObjectsForDerivedSource() throws IOException {
        BytesReference source1 = new BytesArray("{\"field\":{\"nested1\":1,\"nested2\":true}}".getBytes(StandardCharsets.UTF_8));
        BytesReference source2 = new BytesArray("{\"field\":{\"nested2\":true,\"nested1\":1}}".getBytes(StandardCharsets.UTF_8));
        Translog.Index op1 = new Translog.Index("1", 0, 1, source1.toBytesRef().bytes);
        Translog.Index op2 = new Translog.Index("1", 0, 1, source2.toBytesRef().bytes);

        mapperService = createMapperService(
            XContentFactory.jsonBuilder()
                .startObject()
                .startObject("properties")
                .startObject("field")
                .startObject("properties")
                .startObject("nested1")
                .field("type", "integer")
                .endObject()
                .startObject("nested2")
                .field("type", "boolean")
                .endObject()
                .endObject()
                .endObject()
                .endObject()
                .endObject()
        );
        documentMapperForType = mock(DocumentMapperForType.class);
        when(documentMapperForType.getDocumentMapper()).thenReturn(mapperService.documentMapper());

        assertTrue(
            "Sources with nested objects in different order should be equal for derived source case",
            helperWithDerivedSource.hasSameIndexOperation(op1, op2)
        );
    }

    public void testSourceComparisonWithArrays() {
        BytesReference source1 = new BytesArray("{\"array\":[1,2,3]}".getBytes(StandardCharsets.UTF_8));
        BytesReference source2 = new BytesArray("{\"array\":[1,2,3]}".getBytes(StandardCharsets.UTF_8));
        BytesReference source3 = new BytesArray("{\"array\":[3,2,1]}".getBytes(StandardCharsets.UTF_8));

        assertTrue("Identical arrays should be equal", TranslogOperationHelper.compareSourcesWithOrder(source1, source2));
        assertFalse("Different arrays should not be equal", TranslogOperationHelper.compareSourcesWithOrder(source1, source3));
    }

    public void testSourceComparisonWithMixedTypes() {
        BytesReference source1 = new BytesArray(
            "{\"string\":\"value\",\"number\":42,\"bool\":true,\"null\":null}".getBytes(StandardCharsets.UTF_8)
        );
        BytesReference source2 = new BytesArray(
            "{\"null\":null,\"bool\":true,\"number\":42,\"string\":\"value\"}".getBytes(StandardCharsets.UTF_8)
        );

        assertTrue(
            "Sources with mixed types in different order should be equal",
            TranslogOperationHelper.compareSourcesWithOrder(source1, source2)
        );
    }

    public void testEmptyHelper() {
        BytesReference source = new BytesArray("{\"field\":\"value\"}".getBytes(StandardCharsets.UTF_8));
        Translog.Index op1 = new Translog.Index("1", 0, 1, source.toBytesRef().bytes);
        Translog.Index op2 = new Translog.Index("1", 0, 1, source.toBytesRef().bytes);

        assertTrue("Empty helper should compare operations normally", TranslogOperationHelper.DEFAULT.hasSameIndexOperation(op1, op2));
    }

    public void testDifferentRoutingNormalCase() {
        BytesReference source = new BytesArray("{\"field\":\"value\"}".getBytes(StandardCharsets.UTF_8));
        Translog.Index op1 = new Translog.Index("1", 0, 1, 0, source.toBytesRef().bytes, "route1", 1);
        Translog.Index op2 = new Translog.Index("1", 0, 1, 0, source.toBytesRef().bytes, "route2", 1);

        assertFalse("Operations with different routing should not be equal", helper.hasSameIndexOperation(op1, op2));
    }

    public void testNullRoutingNormalCase() {
        BytesReference source = new BytesArray("{\"field\":\"value\"}".getBytes(StandardCharsets.UTF_8));
        Translog.Index op1 = new Translog.Index("1", 0, 1, 0, source.toBytesRef().bytes, null, 1);
        Translog.Index op2 = new Translog.Index("1", 0, 1, 0, source.toBytesRef().bytes, "route", 1);

        assertFalse("Operations with null and non-null routing should not be equal", helper.hasSameIndexOperation(op1, op2));
    }

    public void testDifferentPrimaryTermNormalCase() {
        BytesReference source = new BytesArray("{\"field\":\"value\"}".getBytes(StandardCharsets.UTF_8));
        Translog.Index op1 = new Translog.Index("1", 0, 1, source.toBytesRef().bytes);
        Translog.Index op2 = new Translog.Index("1", 0, 2, source.toBytesRef().bytes);

        assertFalse("Operations with different primary terms should not be equal", helper.hasSameIndexOperation(op1, op2));
    }

    public void testSourceComparisonWithDifferentContentTypes() throws IOException {
        // JSON format
        BytesReference jsonSource = new BytesArray("{\"field\":\"value\", \"number\":1}".getBytes(StandardCharsets.UTF_8));

        // YAML format
        BytesReference yamlSource = BytesReference.bytes(
            XContentFactory.yamlBuilder().startObject().field("field", "value").field("number", 1).endObject()
        );

        // SMILE format (binary JSON)
        BytesReference smileSource = BytesReference.bytes(
            XContentFactory.smileBuilder().startObject().field("field", "value").field("number", 1).endObject()
        );

        // CBOR format
        BytesReference cborSource = BytesReference.bytes(
            XContentFactory.cborBuilder().startObject().field("field", "value").field("number", 1).endObject()
        );

        // Test different combinations
        assertTrue(
            "JSON and YAML with same content should be equal",
            TranslogOperationHelper.compareSourcesWithOrder(jsonSource, yamlSource)
        );

        assertTrue(
            "JSON and SMILE with same content should be equal",
            TranslogOperationHelper.compareSourcesWithOrder(jsonSource, smileSource)
        );

        assertTrue(
            "JSON and CBOR with same content should be equal",
            TranslogOperationHelper.compareSourcesWithOrder(jsonSource, cborSource)
        );

        assertTrue(
            "YAML and SMILE with same content should be equal",
            TranslogOperationHelper.compareSourcesWithOrder(yamlSource, smileSource)
        );

        assertTrue(
            "YAML and CBOR with same content should be equal",
            TranslogOperationHelper.compareSourcesWithOrder(yamlSource, cborSource)
        );

        assertTrue(
            "SMILE and CBOR with same content should be equal",
            TranslogOperationHelper.compareSourcesWithOrder(smileSource, cborSource)
        );
    }

    public void testSourceComparisonWithNullValues() {
        BytesReference source = new BytesArray("{\"field\":\"value\"}".getBytes(StandardCharsets.UTF_8));

        assertFalse("Null and non-null sources should not be equal", TranslogOperationHelper.compareSourcesWithOrder(null, source));
        assertFalse("Non-null and null sources should not be equal", TranslogOperationHelper.compareSourcesWithOrder(source, null));
        assertTrue("Null sources should be equal", TranslogOperationHelper.compareSourcesWithOrder(null, null));
    }

    public void testSourceComparisonWithInvalidContent() {
        BytesReference validJson = new BytesArray("{\"field\":\"value\"}".getBytes(StandardCharsets.UTF_8));
        BytesReference invalidContent = new BytesArray("invalid content".getBytes(StandardCharsets.UTF_8));

        assertFalse(
            "Valid and invalid content should not be equal",
            TranslogOperationHelper.compareSourcesWithOrder(validJson, invalidContent)
        );
    }
}
