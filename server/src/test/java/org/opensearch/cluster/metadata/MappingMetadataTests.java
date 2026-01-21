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
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.metadata.index.model.MappingMetadataModel;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class MappingMetadataTests extends OpenSearchTestCase {

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

    public void testModelDeserialization() throws IOException {
        Map<String, Object> mapping = createTestMapping();
        final MappingMetadata mappingMetadata = new MappingMetadata("_doc", mapping);

        // Serialize using MappingMetadata
        final BytesStreamOutput out = new BytesStreamOutput();
        mappingMetadata.writeTo(out);

        // Deserialize using MappingMetadataModel
        final StreamInput in = out.bytes().streamInput();
        final MappingMetadataModel model = new MappingMetadataModel(in);

        // Verify all fields match
        assertThat(model.type(), equalTo(mappingMetadata.type()));
        assertThat(model.routingRequired(), equalTo(mappingMetadata.routingRequired()));

        // Verify source content matches
        assertNotNull(model.source());
        assertArrayEquals(mappingMetadata.source().compressed(), model.source().compressedBytes());
    }

    public void testModelDeserializationWithRoutingRequired() throws IOException {
        Map<String, Object> mapping = createTestMappingWithRouting(true);
        final MappingMetadata mappingMetadata = new MappingMetadata("_doc", mapping);

        // Serialize using MappingMetadata
        final BytesStreamOutput out = new BytesStreamOutput();
        mappingMetadata.writeTo(out);

        // Deserialize using MappingMetadataModel
        final StreamInput in = out.bytes().streamInput();
        final MappingMetadataModel model = new MappingMetadataModel(in);

        // Verify routing required is preserved
        assertTrue(model.routingRequired());
        assertThat(model.type(), equalTo("_doc"));
    }

    public void testModelToMetadataSerialization() throws IOException {
        // Create a MappingMetadata, serialize it
        Map<String, Object> mapping = createTestMapping();
        final MappingMetadata original = new MappingMetadata("_doc", mapping);

        final BytesStreamOutput out1 = new BytesStreamOutput();
        original.writeTo(out1);

        // Deserialize as MappingMetadataModel
        final StreamInput in1 = out1.bytes().streamInput();
        final MappingMetadataModel model = new MappingMetadataModel(in1);

        // Serialize the model
        final BytesStreamOutput out2 = new BytesStreamOutput();
        model.writeTo(out2);

        // Deserialize as MappingMetadata
        final StreamInput in2 = out2.bytes().streamInput();
        final MappingMetadata restored = new MappingMetadata(in2);

        // Verify round-trip preserves data
        assertThat(restored.type(), equalTo(original.type()));
        assertThat(restored.routingRequired(), equalTo(original.routingRequired()));
        assertArrayEquals(restored.source().compressed(), original.source().compressed());
    }

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
        Map<String, Object> docMapping = createTestMappingContent();
        mapping.put("_doc", docMapping);

        CompressedXContent compressedMapping = new CompressedXContent(BytesReference.bytes(XContentFactory.jsonBuilder().map(mapping)));
        final MappingMetadata before = new MappingMetadata(compressedMapping);

        assertThat(before.type(), equalTo("_doc"));

        final BytesStreamOutput out = new BytesStreamOutput();
        before.writeTo(out);

        final StreamInput in = out.bytes().streamInput();
        final MappingMetadata after = new MappingMetadata(in);

        assertThat(after, equalTo(before));
    }

    public void testModelAccessor() throws IOException {
        Map<String, Object> mapping = createTestMappingWithRouting(true);
        final MappingMetadata mappingMetadata = new MappingMetadata("_doc", mapping);

        // Verify model() accessor returns the underlying model
        MappingMetadataModel model = mappingMetadata.model();
        assertNotNull(model);
        assertThat(model.type(), equalTo("_doc"));
        assertTrue(model.routingRequired());
        assertNotNull(model.source());
    }

    public void testConstructorFromModel() throws IOException {
        // Create original MappingMetadata
        Map<String, Object> mapping = createTestMappingWithRouting(true);
        final MappingMetadata original = new MappingMetadata("_doc", mapping);

        // Get the model and create new MappingMetadata from it
        MappingMetadataModel model = original.model();
        MappingMetadata fromModel = new MappingMetadata(model);

        // Verify all fields match
        assertThat(fromModel.type(), equalTo(original.type()));
        assertThat(fromModel.routingRequired(), equalTo(original.routingRequired()));
        assertArrayEquals(fromModel.source().compressed(), original.source().compressed());

        // Verify equals
        assertThat(fromModel, equalTo(original));
    }

    public void testModelRoundTrip() throws IOException {
        Map<String, Object> mapping = createTestMappingWithRouting(false);
        final MappingMetadata original = new MappingMetadata("_doc", mapping);

        // Serialize MappingMetadata
        final BytesStreamOutput out1 = new BytesStreamOutput();
        original.writeTo(out1);

        // Deserialize as MappingMetadataModel
        final StreamInput in1 = out1.bytes().streamInput();
        final MappingMetadataModel model = new MappingMetadataModel(in1);

        // Create new MappingMetadata from model
        MappingMetadata fromModel = new MappingMetadata(model);

        // Serialize the new MappingMetadata
        final BytesStreamOutput out2 = new BytesStreamOutput();
        fromModel.writeTo(out2);

        // Deserialize back to MappingMetadata
        final StreamInput in2 = out2.bytes().streamInput();
        final MappingMetadata restored = new MappingMetadata(in2);

        // Verify round-trip preserves all data
        assertThat(restored, equalTo(original));
        assertThat(restored.type(), equalTo(original.type()));
        assertThat(restored.routingRequired(), equalTo(original.routingRequired()));
    }

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
