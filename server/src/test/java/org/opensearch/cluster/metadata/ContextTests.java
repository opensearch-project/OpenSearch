/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.metadata.index.model.ContextModel;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class ContextTests extends OpenSearchTestCase {

    // --- Constructor tests ---

    public void testSimpleConstructor() {
        Context context = new Context("simple-context");

        assertThat(context.name(), equalTo("simple-context"));
        assertThat(context.version(), equalTo(Context.LATEST_VERSION));
        assertTrue(context.params().isEmpty());
    }

    public void testFullConstructorWithNullVersion() {
        Context context = new Context("test", null, Map.of());
        assertThat(context.version(), equalTo(Context.LATEST_VERSION));
    }

    public void testFullConstructorWithExplicitVersion() {
        Context context = new Context("test", "2.0", Map.of("key", "val"));
        assertThat(context.name(), equalTo("test"));
        assertThat(context.version(), equalTo("2.0"));
        assertThat(context.params().get("key"), equalTo("val"));
    }

    public void testContextFromModel() {
        ContextModel model = new ContextModel("model-context", "2.0", Map.of("param", "value"));
        Context context = new Context(model);

        assertThat(context.name(), equalTo("model-context"));
        assertThat(context.version(), equalTo("2.0"));
        assertThat(context.params().get("param"), equalTo("value"));
        assertSame(model, context.model());
    }

    public void testContextFromModelWithNullVersion() {
        // ContextModel defaults null version to LATEST_VERSION
        ContextModel model = new ContextModel("test", null, Map.of());
        Context context = new Context(model);

        assertThat(context.version(), equalTo(Context.LATEST_VERSION));
    }

    public void testLatestVersionDelegatesToModel() {
        assertThat(Context.LATEST_VERSION, equalTo(ContextModel.LATEST_VERSION));
    }

    // --- Serialization tests ---

    public void testSerialization() throws IOException {
        final Context before = createTestContext();

        final BytesStreamOutput out = new BytesStreamOutput();
        before.writeTo(out);

        final StreamInput in = out.bytes().streamInput();
        final Context after = new Context(in);

        assertThat(after, equalTo(before));
        assertThat(after.name(), equalTo(before.name()));
        assertThat(after.version(), equalTo(before.version()));
        assertThat(after.params(), equalTo(before.params()));
    }

    public void testSerializationWithParams() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("key1", "value1");
        params.put("key2", 42);
        params.put("key3", true);

        final Context before = new Context("test-context", "1.0", params);

        final BytesStreamOutput out = new BytesStreamOutput();
        before.writeTo(out);

        final StreamInput in = out.bytes().streamInput();
        final Context after = new Context(in);

        assertThat(after, equalTo(before));
        assertThat(after.params().get("key1"), equalTo("value1"));
        assertThat(after.params().get("key2"), equalTo(42));
        assertThat(after.params().get("key3"), equalTo(true));
    }

    public void testSerializationWithMixedParamTypes() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("string", "hello");
        params.put("int", 123);
        params.put("long", 9876543210L);
        params.put("double", 3.14159);
        params.put("boolean", false);

        final Context before = new Context("mixed-context", "2.0", params);

        final BytesStreamOutput out = new BytesStreamOutput();
        before.writeTo(out);

        final StreamInput in = out.bytes().streamInput();
        final Context after = new Context(in);

        assertThat(after, equalTo(before));
        assertThat(after.params().get("string"), equalTo("hello"));
        assertThat(after.params().get("int"), equalTo(123));
        assertThat(after.params().get("long"), equalTo(9876543210L));
        assertThat(after.params().get("double"), equalTo(3.14159));
        assertThat(after.params().get("boolean"), equalTo(false));
    }

    public void testSerializationWithNullVersion() throws IOException {
        final Context before = new Context("test-context", null, Collections.emptyMap());
        assertThat(before.version(), equalTo(Context.LATEST_VERSION));

        final BytesStreamOutput out = new BytesStreamOutput();
        before.writeTo(out);

        final StreamInput in = out.bytes().streamInput();
        final Context after = new Context(in);

        assertThat(after, equalTo(before));
        assertThat(after.version(), equalTo(Context.LATEST_VERSION));
    }

    // --- Model interop tests ---

    public void testModelDeserialization() throws IOException {
        final Context context = createTestContext();

        final BytesStreamOutput out = new BytesStreamOutput();
        context.writeTo(out);

        final StreamInput in = out.bytes().streamInput();
        final ContextModel model = new ContextModel(in);

        assertThat(model.name(), equalTo(context.name()));
        assertThat(model.version(), equalTo(context.version()));
        assertThat(model.params(), equalTo(context.params()));
    }

    public void testModelToContextSerialization() throws IOException {
        final Context original = createTestContext();

        final BytesStreamOutput out1 = new BytesStreamOutput();
        original.writeTo(out1);

        final StreamInput in1 = out1.bytes().streamInput();
        final ContextModel model = new ContextModel(in1);

        final BytesStreamOutput out2 = new BytesStreamOutput();
        model.writeTo(out2);

        final StreamInput in2 = out2.bytes().streamInput();
        final Context restored = new Context(in2);

        assertThat(restored.name(), equalTo(original.name()));
        assertThat(restored.version(), equalTo(original.version()));
        assertThat(restored.params(), equalTo(original.params()));
    }

    // --- XContent tests ---

    public void testToXContent() throws IOException {
        Map<String, Object> params = new HashMap<>();
        params.put("key", "value");
        Context context = new Context("test-context", "1.0", params);

        XContentBuilder builder = XContentFactory.jsonBuilder();
        context.toXContent(builder, null);
        String json = builder.toString();

        assertTrue(json.contains("\"name\":\"test-context\""));
        assertTrue(json.contains("\"version\":\"1.0\""));
        assertTrue(json.contains("\"params\":{\"key\":\"value\"}"));
    }

    public void testToXContentAlwaysWritesVersion() throws IOException {
        Context context = new Context("test", null, Map.of());

        XContentBuilder builder = XContentFactory.jsonBuilder();
        context.toXContent(builder, null);
        String json = builder.toString();

        assertTrue(json.contains("\"version\":\"" + Context.LATEST_VERSION + "\""));
    }

    public void testFromXContent() throws IOException {
        String json = "{\"name\":\"parsed-context\",\"version\":\"2.0\",\"params\":{\"key\":\"value\"}}";

        XContentParser parser = createParser(XContentType.JSON.xContent(), json);
        Context context = Context.fromXContent(parser);

        assertThat(context.name(), equalTo("parsed-context"));
        assertThat(context.version(), equalTo("2.0"));
        assertThat(context.params().get("key"), equalTo("value"));
    }

    public void testFromXContentWithoutVersion() throws IOException {
        String json = "{\"name\":\"parsed-context\"}";

        XContentParser parser = createParser(XContentType.JSON.xContent(), json);
        Context context = Context.fromXContent(parser);

        assertThat(context.name(), equalTo("parsed-context"));
        assertThat(context.version(), equalTo(Context.LATEST_VERSION));
    }

    public void testFromXContentWithoutParams() throws IOException {
        String json = "{\"name\":\"test\",\"version\":\"1.0\"}";

        XContentParser parser = createParser(XContentType.JSON.xContent(), json);
        Context context = Context.fromXContent(parser);

        assertThat(context.name(), equalTo("test"));
        assertThat(context.version(), equalTo("1.0"));
        assertNull(context.params());
    }

    public void testXContentRoundTrip() throws IOException {
        Context original = new Context("round-trip", "3.0", Map.of("k", "v"));

        XContentBuilder builder = XContentFactory.jsonBuilder();
        original.toXContent(builder, null);
        String json = builder.toString();

        XContentParser parser = createParser(XContentType.JSON.xContent(), json);
        Context parsed = Context.fromXContent(parser);

        assertEquals(original, parsed);
    }

    // --- Equals/hashCode tests ---

    public void testEquals() {
        Context context1 = new Context("test", "1.0", Map.of("key", "value"));
        Context context2 = new Context("test", "1.0", Map.of("key", "value"));

        assertNotSame(context1, context2);
        assertEquals(context1, context2);
        assertEquals(context1.hashCode(), context2.hashCode());
    }

    public void testNotEquals() {
        Context context1 = new Context("name1", "1.0", Collections.emptyMap());
        Context context2 = new Context("name2", "1.0", Collections.emptyMap());
        Context context3 = new Context("name1", "2.0", Collections.emptyMap());
        Context context4 = new Context("name1", "1.0", Map.of("key", "value"));

        assertNotEquals(context1, context2);
        assertNotEquals(context1, context3);
        assertNotEquals(context1, context4);
    }

    // --- toString test ---

    public void testToString() {
        Context context = new Context("test-context", "1.0", Map.of("key", "value"));
        String str = context.toString();

        assertTrue(str.contains("test-context"));
        assertTrue(str.contains("1.0"));
        assertTrue(str.contains("key"));
        assertTrue(str.contains("value"));
    }

    // --- Helper ---

    private Context createTestContext() {
        String name = randomAlphaOfLengthBetween(3, 10);
        String version = randomBoolean() ? randomAlphaOfLengthBetween(1, 5) : Context.LATEST_VERSION;
        Map<String, Object> params = new HashMap<>();
        if (randomBoolean()) {
            int numParams = randomIntBetween(1, 5);
            for (int i = 0; i < numParams; i++) {
                params.put(randomAlphaOfLengthBetween(3, 10), randomAlphaOfLengthBetween(3, 20));
            }
        }
        return new Context(name, version, params);
    }
}
