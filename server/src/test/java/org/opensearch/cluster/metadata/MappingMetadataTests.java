/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.BufferedChecksumStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.metadata.index.model.MappingMetadataModel;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class MappingMetadataTests extends OpenSearchTestCase {

    // --- Serialization tests ---

    public void testSerialization() throws IOException {
        Map<String, Object> mapping = createTestMapping();
        final MappingMetadata before = new MappingMetadata("_doc", mapping);

        final BytesStreamOutput out = new BytesStreamOutput();
        before.writeTo(out);

        final StreamInput in = out.bytes().streamInput();
        final MappingMetadata after = new MappingMetadata(in);

        assertThat(after, equalTo(before));
        assertThat(after.type(), equalTo(before.type()));
        assertThat(after.routingRequired(), equalTo(before.routingRequired()));
    }

    public void testSerializationWithRoutingRequired() throws IOException {
        Map<String, Object> mapping = createTestMappingWithRouting(true);
        final MappingMetadata before = new MappingMetadata("_doc", mapping);

        assertTrue(before.routingRequired());

        final BytesStreamOutput out = new BytesStreamOutput();
        before.writeTo(out);

        final StreamInput in = out.bytes().streamInput();
        final MappingMetadata after = new MappingMetadata(in);

        assertThat(after, equalTo(before));
        assertTrue(after.routingRequired());
    }

    // --- Model interop tests ---

    public void testModelDeserialization() throws IOException {
        Map<String, Object> mapping = createTestMapping();
        final MappingMetadata mappingMetadata = new MappingMetadata("_doc", mapping);

        final BytesStreamOutput out = new BytesStreamOutput();
        mappingMetadata.writeTo(out);

        final StreamInput in = out.bytes().streamInput();
        final MappingMetadataModel model = new MappingMetadataModel(in);

        assertThat(model.type(), equalTo(mappingMetadata.type()));
        assertThat(model.routingRequired(), equalTo(mappingMetadata.routingRequired()));
        assertNotNull(model.source());
        assertArrayEquals(mappingMetadata.source().compressed(), model.source().compressedBytes());
    }

    public void testModelDeserializationWithRoutingRequired() throws IOException {
        Map<String, Object> mapping = createTestMappingWithRouting(true);
        final MappingMetadata mappingMetadata = new MappingMetadata("_doc", mapping);

        final BytesStreamOutput out = new BytesStreamOutput();
        mappingMetadata.writeTo(out);

        final StreamInput in = out.bytes().streamInput();
        final MappingMetadataModel model = new MappingMetadataModel(in);

        assertTrue(model.routingRequired());
        assertThat(model.type(), equalTo("_doc"));
    }

    public void testModelToMetadataSerialization() throws IOException {
        Map<String, Object> mapping = createTestMapping();
        final MappingMetadata original = new MappingMetadata("_doc", mapping);

        final BytesStreamOutput out1 = new BytesStreamOutput();
        original.writeTo(out1);

        final StreamInput in1 = out1.bytes().streamInput();
        final MappingMetadataModel model = new MappingMetadataModel(in1);

        final BytesStreamOutput out2 = new BytesStreamOutput();
        model.writeTo(out2);

        final StreamInput in2 = out2.bytes().streamInput();
        final MappingMetadata restored = new MappingMetadata(in2);

        assertThat(restored.type(), equalTo(original.type()));
        assertThat(restored.routingRequired(), equalTo(original.routingRequired()));
        assertArrayEquals(restored.source().compressed(), original.source().compressed());
    }

    public void testConstructorFromModel() throws IOException {
        Map<String, Object> mapping = createTestMappingWithRouting(true);
        final MappingMetadata original = new MappingMetadata("_doc", mapping);

        MappingMetadata fromModel = new MappingMetadata(original.model());

        assertThat(fromModel, equalTo(original));
        assertThat(fromModel.type(), equalTo(original.type()));
        assertThat(fromModel.routingRequired(), equalTo(original.routingRequired()));
    }

    public void testModelAccessor() {
        Map<String, Object> mapping = createTestMappingWithRouting(true);
        final MappingMetadata mm = new MappingMetadata("_doc", mapping);

        MappingMetadataModel model = mm.model();
        assertNotNull(model);
        assertThat(model.type(), equalTo("_doc"));
        assertTrue(model.routingRequired());
    }

    // --- Constructor tests ---

    public void testEmptyMappings() throws IOException {
        final MappingMetadata before = MappingMetadata.EMPTY_MAPPINGS;

        final BytesStreamOutput out = new BytesStreamOutput();
        before.writeTo(out);

        final StreamInput in = out.bytes().streamInput();
        final MappingMetadata after = new MappingMetadata(in);

        assertThat(after.type(), equalTo(before.type()));
        assertThat(after.routingRequired(), equalTo(before.routingRequired()));
    }

    public void testCompressedXContentConstructor() throws IOException {
        Map<String, Object> mapping = new HashMap<>();
        mapping.put("_doc", createTestMappingContent());

        CompressedXContent compressedMapping = new CompressedXContent(BytesReference.bytes(XContentFactory.jsonBuilder().map(mapping)));
        final MappingMetadata mm = new MappingMetadata(compressedMapping);

        assertThat(mm.type(), equalTo("_doc"));

        final BytesStreamOutput out = new BytesStreamOutput();
        mm.writeTo(out);

        final StreamInput in = out.bytes().streamInput();
        final MappingMetadata after = new MappingMetadata(in);

        assertThat(after, equalTo(mm));
    }

    // --- sourceAsMap tests ---

    public void testSourceAsMap() {
        Map<String, Object> mapping = createTestMapping();
        MappingMetadata mm = new MappingMetadata("_doc", mapping);

        Map<String, Object> sourceMap = mm.sourceAsMap();
        // sourceAsMap reduces the type wrapper
        assertTrue(sourceMap.containsKey("properties"));
        assertFalse(sourceMap.containsKey("_doc"));
    }

    public void testGetSourceAsMapDelegatesToSourceAsMap() {
        Map<String, Object> mapping = createTestMapping();
        MappingMetadata mm = new MappingMetadata("_doc", mapping);

        assertEquals(mm.sourceAsMap(), mm.getSourceAsMap());
    }

    // --- Equals/hashCode tests ---

    public void testEquals() {
        Map<String, Object> mapping = createTestMapping();
        MappingMetadata m1 = new MappingMetadata("_doc", mapping);
        MappingMetadata m2 = new MappingMetadata("_doc", mapping);

        assertEquals(m1, m2);
        assertEquals(m1.hashCode(), m2.hashCode());
    }

    public void testNotEqualsNull() {
        assertNotEquals(new MappingMetadata("_doc", createTestMapping()), null);
    }

    public void testNotEqualsDifferentType() {
        assertNotEquals(new MappingMetadata("_doc", createTestMapping()), "a string");
    }

    public void testNotEqualsDifferentRouting() {
        MappingMetadata m1 = new MappingMetadata("_doc", createTestMappingWithRouting(true));
        MappingMetadata m2 = new MappingMetadata("_doc", createTestMappingWithRouting(false));
        assertNotEquals(m1, m2);
    }

    // --- writeVerifiableTo test ---

    public void testWriteVerifiableTo() throws IOException {
        Map<String, Object> mapping = createTestMappingWithRouting(true);
        MappingMetadata mm = new MappingMetadata("_doc", mapping);

        BytesStreamOutput bso = new BytesStreamOutput();
        BufferedChecksumStreamOutput out = new BufferedChecksumStreamOutput(bso);
        mm.writeVerifiableTo(out);
        out.flush();

        assertTrue(bso.bytes().length() > 0);
    }

    // --- Helpers ---

    private Map<String, Object> createTestMapping() {
        Map<String, Object> mapping = new HashMap<>();
        mapping.put("_doc", createTestMappingContent());
        return mapping;
    }

    private Map<String, Object> createTestMappingContent() {
        Map<String, Object> properties = new HashMap<>();

        Map<String, Object> titleField = new HashMap<>();
        titleField.put("type", "text");
        properties.put("title", titleField);

        Map<String, Object> countField = new HashMap<>();
        countField.put("type", "integer");
        properties.put("count", countField);

        Map<String, Object> content = new HashMap<>();
        content.put("properties", properties);
        return content;
    }

    private Map<String, Object> createTestMappingWithRouting(boolean required) {
        Map<String, Object> content = createTestMappingContent();

        Map<String, Object> routing = new HashMap<>();
        routing.put("required", required);
        content.put("_routing", routing);

        Map<String, Object> mapping = new HashMap<>();
        mapping.put("_doc", content);
        return mapping;
    }
}
