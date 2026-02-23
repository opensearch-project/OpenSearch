/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.metadata.index.model;

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.metadata.common.XContentContext;
import org.opensearch.metadata.compress.CompressedData;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class MappingMetadataModelTests extends OpenSearchTestCase {

    // --- Constructor tests ---

    public void testConstructorNullTypeThrows() {
        expectThrows(NullPointerException.class, () -> new MappingMetadataModel(null, createJsonSource("t", "{}"), false));
    }

    public void testConstructorNullSourceThrows() {
        expectThrows(NullPointerException.class, () -> new MappingMetadataModel("_doc", null, false));
    }

    // --- Serialization tests ---

    public void testSerialization() throws IOException {
        final MappingMetadataModel before = createTestItem();

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final StreamOutput out = new OutputStreamStreamOutput(baos);
        before.writeTo(out);
        out.close();

        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        final StreamInput in = new InputStreamStreamInput(bais);
        final MappingMetadataModel after = new MappingMetadataModel(in);

        assertThat(after, equalTo(before));
        assertThat(after.type(), equalTo(before.type()));
        assertThat(after.source(), equalTo(before.source()));
        assertThat(after.routingRequired(), equalTo(before.routingRequired()));
        assertEquals(before.hashCode(), after.hashCode());
    }

    // --- Equals/hashCode tests ---

    public void testEquals() {
        MappingMetadataModel model1 = createTestItem();
        MappingMetadataModel model2 = new MappingMetadataModel(model1.type(), model1.source(), model1.routingRequired());

        assertNotSame(model1, model2);
        assertEquals(model1, model2);
        assertEquals(model1.hashCode(), model2.hashCode());
    }

    public void testNotEqualsNull() {
        assertNotEquals(createTestItem(), null);
    }

    public void testNotEqualsDifferentType() {
        assertNotEquals(createTestItem(), "a string");
    }

    public void testNotEqualsDifferentMappingType() throws IOException {
        CompressedData source = createJsonSource("_doc", "{\"properties\":{}}");
        MappingMetadataModel m1 = new MappingMetadataModel("type_a", source, false);
        MappingMetadataModel m2 = new MappingMetadataModel("type_b", source, false);
        assertNotEquals(m1, m2);
    }

    public void testNotEqualsDifferentRouting() throws IOException {
        CompressedData source = createJsonSource("_doc", "{\"properties\":{}}");
        MappingMetadataModel m1 = new MappingMetadataModel("_doc", source, true);
        MappingMetadataModel m2 = new MappingMetadataModel("_doc", source, false);
        assertNotEquals(m1, m2);
    }

    // --- fromXContent tests ---

    public void testFromXContentParsesRoutingRequired() throws IOException {
        String json = "{\"my_type\":{\"_routing\":{\"required\":true},\"properties\":{\"id\":{\"type\":\"keyword\"}}}}";

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, json.getBytes())) {
            parser.nextToken(); // START_OBJECT
            parser.nextToken(); // FIELD_NAME
            MappingMetadataModel parsed = MappingMetadataModel.fromXContent(parser);

            assertEquals("my_type", parsed.type());
            assertTrue(parsed.routingRequired());
        }
    }

    public void testFromXContentRoutingNotRequired() throws IOException {
        String json = "{\"_doc\":{\"properties\":{\"title\":{\"type\":\"text\"}}}}";

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, json.getBytes())) {
            parser.nextToken();
            parser.nextToken();
            MappingMetadataModel parsed = MappingMetadataModel.fromXContent(parser);

            assertEquals("_doc", parsed.type());
            assertFalse(parsed.routingRequired());
        }
    }

    public void testFromXContentRoutingRequiredAsString() throws IOException {
        String json = "{\"_doc\":{\"_routing\":{\"required\":\"true\"},\"properties\":{}}}";

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, json.getBytes())) {
            parser.nextToken();
            parser.nextToken();
            MappingMetadataModel parsed = MappingMetadataModel.fromXContent(parser);

            assertTrue(parsed.routingRequired());
        }
    }

    public void testFromXContentSourceIsCompressed() throws IOException {
        String json = "{\"_doc\":{\"properties\":{\"f\":{\"type\":\"text\"}}}}";

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, json.getBytes())) {
            parser.nextToken();
            parser.nextToken();
            MappingMetadataModel parsed = MappingMetadataModel.fromXContent(parser);

            // Verify source can be decompressed (proves it was properly compressed)
            byte[] uncompressed = parsed.source().uncompressed();
            String sourceJson = new String(uncompressed);
            assertTrue(sourceJson.contains("\"_doc\""));
            assertTrue(sourceJson.contains("\"properties\""));
        }
    }

    // --- toXContent API mode tests ---

    public void testToXContentApiMode() throws IOException {
        String mappingContent = "{\"properties\":{\"field1\":{\"type\":\"text\"}}}";
        CompressedData source = createJsonSource("_doc", mappingContent);
        MappingMetadataModel model = new MappingMetadataModel("_doc", source, false);

        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        model.toXContent(builder, apiParams());
        builder.endObject();
        String json = builderToString(builder);

        // API mode: writes "type": { content without type wrapper }
        assertTrue(json.contains("\"_doc\""));
        assertTrue(json.contains("\"properties\""));
        assertTrue(json.contains("\"field1\""));
    }

    public void testXContentApiRoundTrip() throws IOException {
        String mappingContent = "{\"properties\":{\"field1\":{\"type\":\"text\"},\"field2\":{\"type\":\"keyword\"}}}";
        CompressedData source = createJsonSource("_doc", mappingContent);
        MappingMetadataModel original = new MappingMetadataModel("_doc", source, false);

        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        original.toXContent(builder, apiParams());
        builder.endObject();
        byte[] bytes = BytesReference.toBytes(BytesReference.bytes(builder));

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, bytes)) {
            parser.nextToken(); // START_OBJECT
            parser.nextToken(); // FIELD_NAME
            MappingMetadataModel parsed = MappingMetadataModel.fromXContent(parser);

            assertEquals(original.type(), parsed.type());
            assertEquals(original.routingRequired(), parsed.routingRequired());
        }
    }

    public void testXContentApiRoundTripWithRoutingRequired() throws IOException {
        String mappingContent = "{\"_routing\":{\"required\":true},\"properties\":{\"name\":{\"type\":\"text\"}}}";
        CompressedData source = createJsonSource("_doc", mappingContent);
        MappingMetadataModel original = new MappingMetadataModel("_doc", source, true);

        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        original.toXContent(builder, apiParams());
        builder.endObject();
        byte[] bytes = BytesReference.toBytes(BytesReference.bytes(builder));

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, bytes)) {
            parser.nextToken();
            parser.nextToken();
            MappingMetadataModel parsed = MappingMetadataModel.fromXContent(parser);

            assertEquals("_doc", parsed.type());
            assertTrue(parsed.routingRequired());
        }
    }

    public void testXContentApiMinimalMapping() throws IOException {
        CompressedData source = createJsonSource("_doc", "{}");
        MappingMetadataModel original = new MappingMetadataModel("_doc", source, false);

        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        original.toXContent(builder, apiParams());
        builder.endObject();
        byte[] bytes = BytesReference.toBytes(BytesReference.bytes(builder));

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, bytes)) {
            parser.nextToken();
            parser.nextToken();
            MappingMetadataModel parsed = MappingMetadataModel.fromXContent(parser);

            assertEquals("_doc", parsed.type());
            assertFalse(parsed.routingRequired());
        }
    }

    // --- toXContent GATEWAY mode tests ---

    public void testToXContentGatewayMode() throws IOException {
        String mappingContent = "{\"properties\":{\"field1\":{\"type\":\"text\"}}}";
        CompressedData source = createJsonSource("_doc", mappingContent);
        MappingMetadataModel model = new MappingMetadataModel("_doc", source, false);

        // GATEWAY mode: writes raw map (with type wrapper) as a value, no field name
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startArray();
        model.toXContent(builder, gatewayParams());
        builder.endArray();
        String json = builderToString(builder);

        // Should contain the full source map including type wrapper
        assertTrue(json.contains("\"_doc\""));
        assertTrue(json.contains("\"properties\""));
    }

    public void testToXContentGatewayBinaryMode() throws IOException {
        CompressedData source = createJsonSource("_doc", "{\"properties\":{}}");
        MappingMetadataModel model = new MappingMetadataModel("_doc", source, false);

        // Binary GATEWAY: writes compressed bytes as value
        Map<String, String> params = new HashMap<>();
        params.put(XContentContext.PARAM_KEY, XContentContext.GATEWAY.name());
        params.put("binary", "true");

        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startArray();
        model.toXContent(builder, new ToXContent.MapParams(params));
        builder.endArray();

        // Should produce valid output (binary bytes encoded)
        byte[] bytes = BytesReference.toBytes(BytesReference.bytes(builder));
        assertNotNull(bytes);
        assertTrue(bytes.length > 0);
    }

    // --- Helpers ---

    private static MappingMetadataModel createTestItem() {
        return new MappingMetadataModel(randomAlphaOfLengthBetween(3, 10), createTestSource(), randomBoolean());
    }

    private static CompressedData createTestSource() {
        return new CompressedData(randomByteArrayOfLength(randomIntBetween(10, 100)), randomInt());
    }

    private static CompressedData createJsonSource(String type, String json) throws IOException {
        String wrappedJson = "{\"" + type + "\":" + json + "}";
        return new CompressedData(wrappedJson.getBytes());
    }

    private static ToXContent.Params apiParams() {
        return new ToXContent.MapParams(Collections.singletonMap(XContentContext.PARAM_KEY, XContentContext.API.name()));
    }

    private static ToXContent.Params gatewayParams() {
        return new ToXContent.MapParams(Collections.singletonMap(XContentContext.PARAM_KEY, XContentContext.GATEWAY.name()));
    }

    private static String builderToString(XContentBuilder builder) throws IOException {
        return new String(BytesReference.toBytes(BytesReference.bytes(builder)));
    }
}
