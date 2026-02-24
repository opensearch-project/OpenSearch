/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.metadata.index.model;

import org.opensearch.common.util.set.Sets;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.metadata.compress.CompressedData;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.equalTo;

public class AliasMetadataModelTests extends OpenSearchTestCase {

    // --- Constructor tests ---

    public void testConstructorNullAliasThrows() {
        expectThrows(NullPointerException.class, () -> new AliasMetadataModel(null, null, null, null, null, null));
    }

    public void testCopyConstructor() throws IOException {
        CompressedData filter = createJsonFilter("{\"term\":{\"user\":\"kimchy\"}}");
        AliasMetadataModel original = new AliasMetadataModel("orig", filter, "idx", "search", true, false);
        AliasMetadataModel copy = new AliasMetadataModel(original, "renamed");

        assertEquals("renamed", copy.alias());
        assertEquals(original.filter(), copy.filter());
        assertEquals(original.indexRouting(), copy.indexRouting());
        assertEquals(original.searchRouting(), copy.searchRouting());
        assertEquals(original.writeIndex(), copy.writeIndex());
        assertEquals(original.isHidden(), copy.isHidden());
    }

    public void testFilteringRequired() throws IOException {
        AliasMetadataModel withFilter = new AliasMetadataModel.Builder("a").filter(createJsonFilter("{\"term\":{}}")).build();
        AliasMetadataModel withoutFilter = new AliasMetadataModel.Builder("b").build();

        assertTrue(withFilter.filteringRequired());
        assertFalse(withoutFilter.filteringRequired());
    }

    public void testSearchRoutingValues() {
        AliasMetadataModel model = new AliasMetadataModel.Builder("a").searchRouting("trim,tw , ltw , lw").build();
        assertThat(model.searchRoutingValues(), equalTo(Sets.newHashSet("trim", "tw ", " ltw ", " lw")));
    }

    public void testSearchRoutingValuesNull() {
        AliasMetadataModel model = new AliasMetadataModel.Builder("a").build();
        assertTrue(model.searchRoutingValues().isEmpty());
    }

    // --- Serialization tests ---

    public void testSerialization() throws IOException {
        final AliasMetadataModel before = new AliasMetadataModel.Builder("alias").filter(createTestFilter())
            .indexRouting("indexRouting")
            .routing("routing")
            .searchRouting("trim,tw , ltw , lw")
            .writeIndex(randomBoolean() ? null : randomBoolean())
            .isHidden(randomBoolean() ? null : randomBoolean())
            .build();

        assertThat(before.searchRoutingValues(), equalTo(Sets.newHashSet("trim", "tw ", " ltw ", " lw")));

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final StreamOutput out = new OutputStreamStreamOutput(baos);
        before.writeTo(out);
        out.close();

        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        final StreamInput in = new InputStreamStreamInput(bais);
        final AliasMetadataModel after = new AliasMetadataModel(in);

        assertThat(after, equalTo(before));
        assertEquals(before.hashCode(), after.hashCode());
    }

    // --- Equals/hashCode tests ---

    public void testEquals() {
        AliasMetadataModel model1 = createTestItem();
        AliasMetadataModel model2 = new AliasMetadataModel(
            model1.alias(),
            model1.filter(),
            model1.indexRouting(),
            model1.searchRouting(),
            model1.writeIndex(),
            model1.isHidden()
        );

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

    public void testNotEqualsDifferentAlias() {
        AliasMetadataModel m1 = new AliasMetadataModel.Builder("alias1").build();
        AliasMetadataModel m2 = new AliasMetadataModel.Builder("alias2").build();
        assertNotEquals(m1, m2);
    }

    public void testNotEqualsDifferentRouting() {
        AliasMetadataModel m1 = new AliasMetadataModel.Builder("a").indexRouting("r1").build();
        AliasMetadataModel m2 = new AliasMetadataModel.Builder("a").indexRouting("r2").build();
        assertNotEquals(m1, m2);
    }

    // --- XContent tests ---

    public void testXContentRoundTrip() throws IOException {
        CompressedData filter = createJsonFilter("{\"term\":{\"user\":\"kimchy\"}}");
        AliasMetadataModel original = new AliasMetadataModel.Builder("test-alias").filter(filter)
            .indexRouting("index_route")
            .searchRouting("search_route")
            .writeIndex(true)
            .isHidden(false)
            .build();

        byte[] bytes = toXContentBytes(original, ToXContent.EMPTY_PARAMS);

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, bytes)) {
            parser.nextToken();
            parser.nextToken();
            AliasMetadataModel parsed = AliasMetadataModel.fromXContent(parser);

            assertEquals(original.alias(), parsed.alias());
            assertEquals(original.indexRouting(), parsed.indexRouting());
            assertEquals(original.searchRouting(), parsed.searchRouting());
            assertEquals(original.writeIndex(), parsed.writeIndex());
            assertEquals(original.isHidden(), parsed.isHidden());
            assertNotNull(parsed.filter());
        }
    }

    public void testXContentRoundTripWithoutFilter() throws IOException {
        AliasMetadataModel original = new AliasMetadataModel.Builder("test-alias").indexRouting("index_route")
            .searchRouting("search_route")
            .writeIndex(false)
            .build();

        byte[] bytes = toXContentBytes(original, ToXContent.EMPTY_PARAMS);

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, bytes)) {
            parser.nextToken();
            parser.nextToken();
            AliasMetadataModel parsed = AliasMetadataModel.fromXContent(parser);

            assertEquals(original.alias(), parsed.alias());
            assertEquals(original.indexRouting(), parsed.indexRouting());
            assertEquals(original.searchRouting(), parsed.searchRouting());
            assertEquals(original.writeIndex(), parsed.writeIndex());
            assertNull(parsed.filter());
            assertNull(parsed.isHidden());
        }
    }

    public void testXContentRoundTripMinimal() throws IOException {
        AliasMetadataModel original = new AliasMetadataModel.Builder("minimal-alias").build();

        byte[] bytes = toXContentBytes(original, ToXContent.EMPTY_PARAMS);

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, bytes)) {
            parser.nextToken();
            parser.nextToken();
            AliasMetadataModel parsed = AliasMetadataModel.fromXContent(parser);

            assertEquals("minimal-alias", parsed.alias());
            assertNull(parsed.filter());
            assertNull(parsed.indexRouting());
            assertNull(parsed.searchRouting());
            assertNull(parsed.writeIndex());
            assertNull(parsed.isHidden());
        }
    }

    public void testFromXContentWithRouting() throws IOException {
        String json = "{\"my-alias\":{\"routing\":\"shared_route\"}}";

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, json.getBytes())) {
            parser.nextToken();
            parser.nextToken();
            AliasMetadataModel parsed = AliasMetadataModel.fromXContent(parser);

            assertEquals("my-alias", parsed.alias());
            assertEquals("shared_route", parsed.indexRouting());
            assertEquals("shared_route", parsed.searchRouting());
        }
    }

    public void testToXContentBinaryFilter() throws IOException {
        CompressedData filter = createJsonFilter("{\"term\":{\"status\":\"active\"}}");
        AliasMetadataModel model = new AliasMetadataModel.Builder("test").filter(filter).build();

        Map<String, String> params = new HashMap<>();
        params.put("binary", "true");
        byte[] bytes = toXContentBytes(model, new ToXContent.MapParams(params));

        // Should produce valid output with binary filter
        assertNotNull(bytes);
        assertTrue(bytes.length > 0);
    }

    // --- Builder tests ---

    public void testBuilderFromModel() throws IOException {
        CompressedData filter = createJsonFilter("{\"term\":{}}");
        AliasMetadataModel original = new AliasMetadataModel("a", filter, "idx", "search", true, false);
        AliasMetadataModel copy = new AliasMetadataModel.Builder(original).build();

        assertEquals(original, copy);
    }

    public void testBuilderRouting() {
        AliasMetadataModel model = new AliasMetadataModel.Builder("a").routing("shared").build();
        assertEquals("shared", model.indexRouting());
        assertEquals("shared", model.searchRouting());
    }

    // --- Helpers ---

    private static AliasMetadataModel createTestItem() {
        AliasMetadataModel.Builder builder = new AliasMetadataModel.Builder(randomAlphaOfLengthBetween(3, 10));
        if (randomBoolean()) builder.routing(randomAlphaOfLengthBetween(3, 10));
        if (randomBoolean()) builder.searchRouting(randomAlphaOfLengthBetween(3, 10));
        if (randomBoolean()) builder.indexRouting(randomAlphaOfLengthBetween(3, 10));
        if (randomBoolean()) builder.filter(createTestFilter());
        builder.writeIndex(randomBoolean());
        if (randomBoolean()) builder.isHidden(randomBoolean());
        return builder.build();
    }

    private static CompressedData createTestFilter() {
        return new CompressedData(randomByteArrayOfLength(randomIntBetween(10, 50)), randomInt());
    }

    private static CompressedData createJsonFilter(String json) throws IOException {
        return new CompressedData(json.getBytes());
    }

    private static byte[] toXContentBytes(AliasMetadataModel model, ToXContent.Params params) throws IOException {
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        model.toXContent(builder, params);
        builder.endObject();
        return BytesReference.toBytes(BytesReference.bytes(builder));
    }
}
