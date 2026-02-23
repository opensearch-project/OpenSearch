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
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class ContextModelTests extends OpenSearchTestCase {

    // --- Constructor tests ---

    public void testNullVersionDefaultsToLatest() {
        ContextModel model = new ContextModel("test", null, Map.of());
        assertThat(model.version(), equalTo(ContextModel.LATEST_VERSION));
    }

    public void testExplicitVersionPreserved() {
        ContextModel model = new ContextModel("test", "2.0", Map.of());
        assertThat(model.version(), equalTo("2.0"));
    }

    public void testLatestVersionConstant() {
        assertThat(ContextModel.LATEST_VERSION, equalTo("_latest"));
    }

    // --- Serialization tests ---

    public void testSerialization() throws IOException {
        final ContextModel before = createTestItem();

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final StreamOutput out = new OutputStreamStreamOutput(baos);
        before.writeTo(out);
        out.close();

        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        final StreamInput in = new InputStreamStreamInput(bais);
        final ContextModel after = new ContextModel(in);

        assertThat(after, equalTo(before));
        assertThat(after.name(), equalTo(before.name()));
        assertThat(after.version(), equalTo(before.version()));
        assertThat(after.params(), equalTo(before.params()));
        assertEquals(before.hashCode(), after.hashCode());
    }

    public void testSerializationWithNullVersion() throws IOException {
        // null version gets defaulted to LATEST_VERSION in constructor
        final ContextModel before = new ContextModel("test-context", null, Collections.emptyMap());
        assertThat(before.version(), equalTo(ContextModel.LATEST_VERSION));

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final StreamOutput out = new OutputStreamStreamOutput(baos);
        before.writeTo(out);
        out.close();

        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        final StreamInput in = new InputStreamStreamInput(bais);
        final ContextModel after = new ContextModel(in);

        assertThat(after, equalTo(before));
        assertThat(after.version(), equalTo(ContextModel.LATEST_VERSION));
    }

    public void testSerializationWithParams() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("stringParam", "value");
        params.put("intParam", 42);
        params.put("boolParam", true);

        final ContextModel before = new ContextModel("test-context", "1.0", params);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final StreamOutput out = new OutputStreamStreamOutput(baos);
        before.writeTo(out);
        out.close();

        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        final StreamInput in = new InputStreamStreamInput(bais);
        final ContextModel after = new ContextModel(in);

        assertThat(after, equalTo(before));
        assertEquals("value", after.params().get("stringParam"));
        assertEquals(42, after.params().get("intParam"));
        assertEquals(true, after.params().get("boolParam"));
    }

    public void testSerializationWithMixedParamTypes() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("string", "hello");
        params.put("int", 123);
        params.put("long", 9876543210L);
        params.put("double", 3.14159);
        params.put("boolean", false);
        params.put("list", Arrays.asList("a", "b", "c"));

        final ContextModel before = new ContextModel("mixed-context", "2.0", params);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final StreamOutput out = new OutputStreamStreamOutput(baos);
        before.writeTo(out);
        out.close();

        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        final StreamInput in = new InputStreamStreamInput(bais);
        final ContextModel after = new ContextModel(in);

        assertThat(after, equalTo(before));
        assertEquals("hello", after.params().get("string"));
        assertEquals(123, after.params().get("int"));
        assertEquals(9876543210L, after.params().get("long"));
        assertEquals(3.14159, after.params().get("double"));
        assertEquals(false, after.params().get("boolean"));
        assertEquals(Arrays.asList("a", "b", "c"), after.params().get("list"));
    }

    // --- Equals/hashCode tests ---

    public void testEquals() {
        ContextModel model1 = createTestItem();
        ContextModel model2 = new ContextModel(model1.name(), model1.version(), model1.params());

        assertNotSame(model1, model2);
        assertEquals(model1, model2);
        assertEquals(model1.hashCode(), model2.hashCode());
    }

    public void testNotEquals() {
        ContextModel model1 = new ContextModel("name1", "1.0", Collections.emptyMap());
        ContextModel model2 = new ContextModel("name2", "1.0", Collections.emptyMap());
        ContextModel model3 = new ContextModel("name1", "2.0", Collections.emptyMap());
        ContextModel model4 = new ContextModel("name1", "1.0", Map.of("key", "value"));

        assertNotEquals(model1, model2);
        assertNotEquals(model1, model3);
        assertNotEquals(model1, model4);
    }

    public void testNotEqualsNull() {
        assertNotEquals(new ContextModel("test", "1.0", Map.of()), null);
    }

    public void testNotEqualsDifferentType() {
        assertNotEquals(new ContextModel("test", "1.0", Map.of()), "not a context model");
    }

    // --- XContent round-trip tests ---

    public void testXContentRoundTrip() throws IOException {
        ContextModel original = createTestItem();

        XContentBuilder builder = JsonXContent.contentBuilder();
        original.toXContent(builder, ToXContent.EMPTY_PARAMS);
        byte[] bytes = BytesReference.bytes(builder).toBytesRef().bytes;

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, bytes)) {
            ContextModel parsed = ContextModel.fromXContent(parser);
            assertEquals(original.name(), parsed.name());
            assertEquals(original.version(), parsed.version());
            assertEquals(original.params(), parsed.params());
        }
    }

    public void testXContentRoundTripWithAllFields() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("key1", "value1");
        params.put("key2", "value2");

        ContextModel original = new ContextModel("test-context", "1.0", params);

        XContentBuilder builder = JsonXContent.contentBuilder();
        original.toXContent(builder, ToXContent.EMPTY_PARAMS);
        byte[] bytes = BytesReference.bytes(builder).toBytesRef().bytes;

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, bytes)) {
            ContextModel parsed = ContextModel.fromXContent(parser);
            assertEquals("test-context", parsed.name());
            assertEquals("1.0", parsed.version());
            assertEquals(params, parsed.params());
        }
    }

    public void testXContentRoundTripWithNullVersion() throws IOException {
        // null version defaults to LATEST_VERSION, which is always written
        ContextModel original = new ContextModel("test-context", null, Collections.emptyMap());
        assertThat(original.version(), equalTo(ContextModel.LATEST_VERSION));

        XContentBuilder builder = JsonXContent.contentBuilder();
        original.toXContent(builder, ToXContent.EMPTY_PARAMS);
        byte[] bytes = BytesReference.bytes(builder).toBytesRef().bytes;

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, bytes)) {
            ContextModel parsed = ContextModel.fromXContent(parser);
            assertEquals("test-context", parsed.name());
            assertEquals(ContextModel.LATEST_VERSION, parsed.version());
        }
    }

    public void testXContentRoundTripWithNullParams() throws IOException {
        ContextModel original = new ContextModel("test-context", "1.0", null);

        XContentBuilder builder = JsonXContent.contentBuilder();
        original.toXContent(builder, ToXContent.EMPTY_PARAMS);
        byte[] bytes = BytesReference.bytes(builder).toBytesRef().bytes;

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, bytes)) {
            ContextModel parsed = ContextModel.fromXContent(parser);
            assertEquals("test-context", parsed.name());
            assertEquals("1.0", parsed.version());
            assertNull(parsed.params());
        }
    }

    public void testFromXContentWithNestedParams() throws IOException {
        String json = "{\"name\":\"test\",\"version\":\"1.0\",\"params\":{\"nested\":{\"key\":\"value\"}}}";
        byte[] bytes = json.getBytes();

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, bytes)) {
            ContextModel parsed = ContextModel.fromXContent(parser);
            assertEquals("test", parsed.name());
            assertEquals("1.0", parsed.version());
            assertNotNull(parsed.params());
            assertTrue(parsed.params().containsKey("nested"));
        }
    }

    public void testFromXContentWithoutVersion() throws IOException {
        String json = "{\"name\":\"test\"}";
        byte[] bytes = json.getBytes();

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, bytes)) {
            ContextModel parsed = ContextModel.fromXContent(parser);
            assertEquals("test", parsed.name());
            // null version from parser → constructor defaults to LATEST_VERSION
            assertEquals(ContextModel.LATEST_VERSION, parsed.version());
            assertNull(parsed.params());
        }
    }

    public void testFromXContentWithoutParams() throws IOException {
        String json = "{\"name\":\"test\",\"version\":\"1.0\"}";
        byte[] bytes = json.getBytes();

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, bytes)) {
            ContextModel parsed = ContextModel.fromXContent(parser);
            assertEquals("test", parsed.name());
            assertEquals("1.0", parsed.version());
            assertNull(parsed.params());
        }
    }

    public void testToXContentAlwaysWritesVersion() throws IOException {
        ContextModel model = new ContextModel("test", null, null);

        XContentBuilder builder = JsonXContent.contentBuilder();
        model.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertTrue(json.contains("\"version\":\"" + ContextModel.LATEST_VERSION + "\""));
    }

    public void testToXContentOmitsNullParams() throws IOException {
        ContextModel model = new ContextModel("test", "1.0", null);

        XContentBuilder builder = JsonXContent.contentBuilder();
        model.toXContent(builder, ToXContent.EMPTY_PARAMS);
        String json = builder.toString();

        assertFalse(json.contains("params"));
    }

    // --- Helper ---

    private static ContextModel createTestItem() {
        String name = randomAlphaOfLengthBetween(3, 10);
        String version = randomBoolean() ? randomAlphaOfLengthBetween(1, 5) : null;
        Map<String, Object> params = new HashMap<>();
        if (randomBoolean()) {
            int numParams = randomIntBetween(1, 5);
            for (int i = 0; i < numParams; i++) {
                params.put(randomAlphaOfLengthBetween(3, 10), randomAlphaOfLengthBetween(3, 20));
            }
        }
        return new ContextModel(name, version, params);
    }
}
