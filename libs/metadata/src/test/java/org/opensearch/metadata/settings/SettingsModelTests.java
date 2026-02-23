/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.metadata.settings;

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.common.xcontent.smile.SmileXContent;
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

public class SettingsModelTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        final SettingsModel before = createTestItem();

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final StreamOutput out = new OutputStreamStreamOutput(baos);
        before.writeTo(out);
        out.close();

        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        final StreamInput in = new InputStreamStreamInput(bais);
        final SettingsModel after = new SettingsModel(in);

        assertThat(after, equalTo(before));
        assertThat(after.getSettings(), equalTo(before.getSettings()));
        assertEquals(before.hashCode(), after.hashCode());
    }

    public void testEquals() {
        SettingsModel model1 = createTestItem();
        SettingsModel model2 = new SettingsModel(model1.getSettings());

        assertNotSame(model1, model2);
        assertEquals(model1, model2);
        assertEquals(model1.hashCode(), model2.hashCode());
    }

    public void testEmpty() {
        SettingsModel empty = SettingsModel.EMPTY;
        assertTrue(empty.getSettings().isEmpty());

        Map<String, Object> nullMap = null;
        SettingsModel fromNull = new SettingsModel(nullMap);
        assertTrue(fromNull.getSettings().isEmpty());
        assertEquals(empty, fromNull);
    }

    public void testSerializationEmpty() throws IOException {
        final SettingsModel before = SettingsModel.EMPTY;

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final StreamOutput out = new OutputStreamStreamOutput(baos);
        before.writeTo(out);
        out.close();

        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        final StreamInput in = new InputStreamStreamInput(bais);
        final SettingsModel after = new SettingsModel(in);

        assertThat(after, equalTo(before));
        assertTrue(after.getSettings().isEmpty());
    }

    public void testSerializationWithListValues() throws IOException {
        Map<String, Object> settings = new HashMap<>();
        settings.put("simple.key", "value");
        settings.put("list.key", Arrays.asList("value1", "value2", "value3"));
        settings.put("null.key", null);

        final SettingsModel before = new SettingsModel(settings);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final StreamOutput out = new OutputStreamStreamOutput(baos);
        before.writeTo(out);
        out.close();

        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        final StreamInput in = new InputStreamStreamInput(bais);
        final SettingsModel after = new SettingsModel(in);

        assertThat(after, equalTo(before));
        assertEquals("value", after.getSettings().get("simple.key"));
        assertEquals(Arrays.asList("value1", "value2", "value3"), after.getSettings().get("list.key"));
        assertNull(after.getSettings().get("null.key"));
    }

    public void testSerializationWithNumericValues() throws IOException {
        Map<String, Object> settings = new HashMap<>();
        settings.put("int.key", 42);
        settings.put("long.key", 9876543210L);
        settings.put("double.key", 3.14159);
        settings.put("float.key", 2.5f);
        settings.put("negative.int", -100);
        settings.put("negative.double", -99.99);

        final SettingsModel before = new SettingsModel(settings);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final StreamOutput out = new OutputStreamStreamOutput(baos);
        before.writeTo(out);
        out.close();

        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        final StreamInput in = new InputStreamStreamInput(bais);
        final SettingsModel after = new SettingsModel(in);

        assertThat(after, equalTo(before));
        // Values are normalized to strings for consistency with Settings
        assertEquals("42", after.getSettings().get("int.key"));
        assertEquals("9876543210", after.getSettings().get("long.key"));
        assertEquals("3.14159", after.getSettings().get("double.key"));
        assertEquals("2.5", after.getSettings().get("float.key"));
        assertEquals("-100", after.getSettings().get("negative.int"));
        assertEquals("-99.99", after.getSettings().get("negative.double"));
    }

    public void testSerializationWithBooleanValues() throws IOException {
        Map<String, Object> settings = new HashMap<>();
        settings.put("bool.true", true);
        settings.put("bool.false", false);

        final SettingsModel before = new SettingsModel(settings);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final StreamOutput out = new OutputStreamStreamOutput(baos);
        before.writeTo(out);
        out.close();

        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        final StreamInput in = new InputStreamStreamInput(bais);
        final SettingsModel after = new SettingsModel(in);

        assertThat(after, equalTo(before));
        // Values are normalized to strings for consistency with Settings
        assertEquals("true", after.getSettings().get("bool.true"));
        assertEquals("false", after.getSettings().get("bool.false"));
    }

    public void testSerializationWithMixedTypes() throws IOException {
        Map<String, Object> settings = new HashMap<>();
        settings.put("string.key", "hello");
        settings.put("int.key", 123);
        settings.put("long.key", Long.MAX_VALUE);
        settings.put("double.key", Double.MIN_VALUE);
        settings.put("bool.key", true);
        settings.put("list.key", Arrays.asList("a", "b", "c"));
        settings.put("null.key", null);

        final SettingsModel before = new SettingsModel(settings);

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final StreamOutput out = new OutputStreamStreamOutput(baos);
        before.writeTo(out);
        out.close();

        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        final StreamInput in = new InputStreamStreamInput(bais);
        final SettingsModel after = new SettingsModel(in);

        assertThat(after, equalTo(before));
        assertEquals("hello", after.getSettings().get("string.key"));
        // Values are normalized to strings for consistency with Settings
        assertEquals("123", after.getSettings().get("int.key"));
        assertEquals(String.valueOf(Long.MAX_VALUE), after.getSettings().get("long.key"));
        assertEquals(String.valueOf(Double.MIN_VALUE), after.getSettings().get("double.key"));
        assertEquals("true", after.getSettings().get("bool.key"));
        // Lists are preserved as-is
        assertEquals(Arrays.asList("a", "b", "c"), after.getSettings().get("list.key"));
        assertNull(after.getSettings().get("null.key"));
    }

    private static SettingsModel createTestItem() {
        Map<String, Object> settings = new HashMap<>();
        int numSettings = randomIntBetween(1, 10);
        for (int i = 0; i < numSettings; i++) {
            String key = randomAlphaOfLengthBetween(3, 10) + "." + randomAlphaOfLengthBetween(3, 10);
            if (randomBoolean()) {
                settings.put(key, randomAlphaOfLengthBetween(3, 20));
            } else {
                settings.put(key, Arrays.asList(randomAlphaOfLength(5), randomAlphaOfLength(5)));
            }
        }
        return new SettingsModel(settings);
    }

    // XContent Tests

    public void testXContentRoundTrip() throws IOException {
        SettingsModel original = createTestItem();

        // Serialize to XContent
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        original.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        byte[] bytes = BytesReference.toBytes(BytesReference.bytes(builder));

        // Parse back from XContent
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, bytes)) {
            parser.nextToken(); // START_OBJECT
            SettingsModel parsed = SettingsModel.fromXContent(parser);
            assertEquals(original, parsed);
        }
    }

    public void testXContentRoundTripWithSimpleValues() throws IOException {
        Map<String, Object> settings = new HashMap<>();
        settings.put("index.number_of_shards", "5");
        settings.put("index.number_of_replicas", "1");
        settings.put("index.refresh_interval", "30s");

        SettingsModel original = new SettingsModel(settings);

        // Serialize to XContent
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        original.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        byte[] bytes = BytesReference.toBytes(BytesReference.bytes(builder));

        // Parse back from XContent
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, bytes)) {
            parser.nextToken(); // START_OBJECT
            SettingsModel parsed = SettingsModel.fromXContent(parser);
            assertEquals(original, parsed);
            assertEquals("5", parsed.getSettings().get("index.number_of_shards"));
            assertEquals("1", parsed.getSettings().get("index.number_of_replicas"));
            assertEquals("30s", parsed.getSettings().get("index.refresh_interval"));
        }
    }

    public void testXContentRoundTripWithListValues() throws IOException {
        Map<String, Object> settings = new HashMap<>();
        settings.put("index.routing.allocation.include._tier_preference", Arrays.asList("data_hot", "data_warm"));
        settings.put("simple.key", "value");

        SettingsModel original = new SettingsModel(settings);

        // Serialize to XContent
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        original.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        byte[] bytes = BytesReference.toBytes(BytesReference.bytes(builder));

        // Parse back from XContent
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, bytes)) {
            parser.nextToken(); // START_OBJECT
            SettingsModel parsed = SettingsModel.fromXContent(parser);
            assertEquals(original, parsed);
            assertEquals(
                Arrays.asList("data_hot", "data_warm"),
                parsed.getSettings().get("index.routing.allocation.include._tier_preference")
            );
            assertEquals("value", parsed.getSettings().get("simple.key"));
        }
    }

    public void testXContentRoundTripWithNullValues() throws IOException {
        Map<String, Object> settings = new HashMap<>();
        settings.put("key.with.value", "value");
        settings.put("key.with.null", null);

        SettingsModel original = new SettingsModel(settings);

        // Serialize to XContent
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        original.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        byte[] bytes = BytesReference.toBytes(BytesReference.bytes(builder));

        // Parse back from XContent
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, bytes)) {
            parser.nextToken(); // START_OBJECT
            SettingsModel parsed = SettingsModel.fromXContent(parser);
            assertEquals(original, parsed);
            assertEquals("value", parsed.getSettings().get("key.with.value"));
            assertNull(parsed.getSettings().get("key.with.null"));
        }
    }

    public void testXContentRoundTripEmpty() throws IOException {
        SettingsModel original = SettingsModel.EMPTY;

        // Serialize to XContent
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        original.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        byte[] bytes = BytesReference.toBytes(BytesReference.bytes(builder));

        // Parse back from XContent
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, bytes)) {
            parser.nextToken(); // START_OBJECT
            SettingsModel parsed = SettingsModel.fromXContent(parser);
            assertTrue(parsed.getSettings().isEmpty());
        }
    }

    public void testFromXContentWithNestedObjects() throws IOException {
        // Test parsing nested object structure (like how Settings are sometimes serialized)
        String json = "{\"index\":{\"number_of_shards\":\"5\",\"number_of_replicas\":\"1\"}}";
        byte[] bytes = json.getBytes();

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, bytes)) {
            parser.nextToken(); // START_OBJECT
            SettingsModel parsed = SettingsModel.fromXContent(parser);
            assertEquals("5", parsed.getSettings().get("index.number_of_shards"));
            assertEquals("1", parsed.getSettings().get("index.number_of_replicas"));
        }
    }

    public void testXContentWithSmileFormat() throws IOException {
        Map<String, Object> settings = new HashMap<>();
        settings.put("index.number_of_shards", "5");
        settings.put("index.number_of_replicas", "1");

        SettingsModel original = new SettingsModel(settings);

        // Serialize to SMILE format
        XContentBuilder builder = SmileXContent.contentBuilder();
        builder.startObject();
        original.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        byte[] bytes = BytesReference.toBytes(BytesReference.bytes(builder));

        // Parse back from SMILE format
        try (XContentParser parser = SmileXContent.smileXContent.createParser(null, null, bytes)) {
            parser.nextToken(); // START_OBJECT
            SettingsModel parsed = SettingsModel.fromXContent(parser);
            assertEquals(original, parsed);
        }
    }

    public void testXContentFlatSettingsRoundTrip() throws IOException {
        Map<String, Object> settings = new HashMap<>();
        settings.put("index.number_of_shards", "5");
        settings.put("index.number_of_replicas", "1");
        settings.put("index.routing.allocation.include._tier_preference", Arrays.asList("data_hot", "data_warm"));
        settings.put("index.null_setting", null);

        SettingsModel original = new SettingsModel(settings);

        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        original.toXContent(builder, new ToXContent.MapParams(Collections.singletonMap("flat_settings", "true")));
        builder.endObject();
        byte[] bytes = BytesReference.toBytes(BytesReference.bytes(builder));

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, bytes)) {
            parser.nextToken();
            SettingsModel parsed = SettingsModel.fromXContent(parser);
            assertEquals(original, parsed);
        }
    }

    public void testEqualsEdgeCases() {
        SettingsModel model = createTestItem();
        assertEquals(model, model);
        assertNotEquals(model, null);
        assertNotEquals(model, "not a SettingsModel");

        Map<String, Object> different = new HashMap<>();
        different.put("completely.different.key", "value");
        assertNotEquals(model, new SettingsModel(different));
    }
}
