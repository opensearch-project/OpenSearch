/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.metadata.index.model;

import org.opensearch.Version;
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
import org.opensearch.metadata.settings.SettingsModel;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class IndexMetadataModelTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        final IndexMetadataModel before = createTestItem();

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final StreamOutput out = new OutputStreamStreamOutput(baos);
        before.writeTo(out);
        out.close();

        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        final StreamInput in = new InputStreamStreamInput(bais);
        final IndexMetadataModel after = new IndexMetadataModel(in);

        assertThat(after.index(), equalTo(before.index()));
        assertThat(after.version(), equalTo(before.version()));
        assertThat(after.mappingVersion(), equalTo(before.mappingVersion()));
        assertThat(after.settingsVersion(), equalTo(before.settingsVersion()));
        assertThat(after.aliasesVersion(), equalTo(before.aliasesVersion()));
        assertThat(after.routingNumShards(), equalTo(before.routingNumShards()));
        assertThat(after.state(), equalTo(before.state()));
        assertArrayEquals(before.primaryTerms(), after.primaryTerms());
        assertThat(after.isSystem(), equalTo(before.isSystem()));
        assertEquals(before.hashCode(), after.hashCode());
    }

    public void testSerializationEmpty() throws IOException {
        final IndexMetadataModel before = new IndexMetadataModel.Builder("empty-index").routingNumShards(1)
            .primaryTerms(new long[] { 1 })
            .build();

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final StreamOutput out = new OutputStreamStreamOutput(baos);
        before.writeTo(out);
        out.close();

        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        final StreamInput in = new InputStreamStreamInput(bais);
        final IndexMetadataModel after = new IndexMetadataModel(in);

        assertThat(after.index(), equalTo("empty-index"));
        assertTrue(after.aliases().isEmpty());
        assertTrue(after.mappings().isEmpty());
        assertNull(after.context());
        assertNull(after.ingestionPaused());
    }

    public void testSerializationWithAllFields() throws IOException {
        Map<String, Object> settingsMap = new HashMap<>();
        settingsMap.put("index.number_of_shards", "4");
        settingsMap.put("index.number_of_replicas", "2");

        Set<String> allocationIds = new HashSet<>();
        allocationIds.add("alloc-1");
        allocationIds.add("alloc-2");

        final IndexMetadataModel before = new IndexMetadataModel.Builder("full-index").version(10)
            .mappingVersion(5)
            .settingsVersion(3)
            .aliasesVersion(2)
            .routingNumShards(32)
            .state((byte) 0)
            .primaryTerms(new long[] { 1, 2, 3, 4 })
            .system(true)
            .settings(new SettingsModel(settingsMap))
            .putMapping(new MappingMetadataModel("_doc", new CompressedData(randomByteArrayOfLength(10), randomInt()), true))
            .putAlias(new AliasMetadataModel.Builder("alias1").routing("r1").writeIndex(true).build())
            .putAlias(new AliasMetadataModel.Builder("alias2").indexRouting("idx").build())
            .context(new ContextModel("test-context", "1.0", Collections.singletonMap("key", "value")))
            .putInSyncAllocationIds(0, allocationIds)
            .ingestionPaused(true)
            .build();

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final StreamOutput out = new OutputStreamStreamOutput(baos);
        before.writeTo(out);
        out.close();

        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        final StreamInput in = new InputStreamStreamInput(bais);
        final IndexMetadataModel after = new IndexMetadataModel(in);

        assertThat(after.index(), equalTo(before.index()));
        assertThat(after.version(), equalTo(before.version()));
        assertThat(after.mappingVersion(), equalTo(before.mappingVersion()));
        assertThat(after.settingsVersion(), equalTo(before.settingsVersion()));
        assertThat(after.aliasesVersion(), equalTo(before.aliasesVersion()));
        assertThat(after.routingNumShards(), equalTo(before.routingNumShards()));
        assertThat(after.state(), equalTo(before.state()));
        assertArrayEquals(before.primaryTerms(), after.primaryTerms());
        assertThat(after.isSystem(), equalTo(before.isSystem()));
        assertThat(after.settings(), equalTo(before.settings()));
        assertThat(after.aliases().size(), equalTo(2));
        assertThat(after.mappings().size(), equalTo(1));
        assertNotNull(after.context());
        assertThat(after.context().name(), equalTo("test-context"));
        assertThat(after.inSyncAllocationIds().get(0), equalTo(allocationIds));
        assertThat(after.ingestionPaused(), equalTo(true));
    }

    public void testSerializationWithContext() throws IOException {
        ContextModel context = new ContextModel("ctx", "2.0", Map.of("param1", "value1", "param2", 42));

        final IndexMetadataModel before = new IndexMetadataModel.Builder("context-index").routingNumShards(1)
            .primaryTerms(new long[] { 1 })
            .context(context)
            .build();

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final StreamOutput out = new OutputStreamStreamOutput(baos);
        before.writeTo(out);
        out.close();

        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        final StreamInput in = new InputStreamStreamInput(bais);
        final IndexMetadataModel after = new IndexMetadataModel(in);

        assertNotNull(after.context());
        assertThat(after.context().name(), equalTo("ctx"));
        assertThat(after.context().version(), equalTo("2.0"));
        assertThat(after.context().params().get("param1"), equalTo("value1"));
        assertThat(after.context().params().get("param2"), equalTo(42));
    }

    public void testSerializationWithAliasesAndMappings() throws IOException {
        AliasMetadataModel alias1 = new AliasMetadataModel.Builder("alias1").routing("r1").writeIndex(true).isHidden(false).build();
        AliasMetadataModel alias2 = new AliasMetadataModel.Builder("alias2").indexRouting("idx").searchRouting("search").build();
        MappingMetadataModel mapping = new MappingMetadataModel("_doc", new CompressedData(randomByteArrayOfLength(20), randomInt()), true);

        final IndexMetadataModel before = new IndexMetadataModel.Builder("alias-mapping-index").routingNumShards(1)
            .primaryTerms(new long[] { 1 })
            .putAlias(alias1)
            .putAlias(alias2)
            .putMapping(mapping)
            .build();

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final StreamOutput out = new OutputStreamStreamOutput(baos);
        before.writeTo(out);
        out.close();

        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        final StreamInput in = new InputStreamStreamInput(bais);
        final IndexMetadataModel after = new IndexMetadataModel(in);

        assertThat(after.aliases().size(), equalTo(2));
        assertNotNull(after.aliases().get("alias1"));
        assertNotNull(after.aliases().get("alias2"));
        assertThat(after.aliases().get("alias1").indexRouting(), equalTo("r1"));
        assertThat(after.aliases().get("alias1").searchRouting(), equalTo("r1"));
        assertThat(after.aliases().get("alias1").writeIndex(), equalTo(true));
        assertThat(after.aliases().get("alias2").indexRouting(), equalTo("idx"));
        assertThat(after.aliases().get("alias2").searchRouting(), equalTo("search"));
        assertThat(after.mappings().size(), equalTo(1));
        assertNotNull(after.mappings().get("_doc"));
        assertThat(after.mappings().get("_doc").routingRequired(), equalTo(true));
    }

    public void testBuilderBasic() {
        IndexMetadataModel model = new IndexMetadataModel.Builder("test-index").version(5)
            .mappingVersion(2)
            .settingsVersion(3)
            .aliasesVersion(4)
            .routingNumShards(10)
            .state((byte) 0)
            .primaryTerms(new long[] { 1, 2, 3 })
            .system(true)
            .build();

        assertEquals("test-index", model.index());
        assertEquals(5, model.version());
        assertEquals(2, model.mappingVersion());
        assertEquals(3, model.settingsVersion());
        assertEquals(4, model.aliasesVersion());
        assertEquals(10, model.routingNumShards());
        assertEquals((byte) 0, model.state());
        assertArrayEquals(new long[] { 1, 2, 3 }, model.primaryTerms());
        assertTrue(model.isSystem());
    }

    public void testBuilderWithMappings() {
        MappingMetadataModel mapping = new MappingMetadataModel(
            "_doc",
            new CompressedData(randomByteArrayOfLength(10), randomInt()),
            false
        );

        IndexMetadataModel model = new IndexMetadataModel.Builder("test-index").routingNumShards(1).putMapping(mapping).build();

        assertEquals(1, model.mappings().size());
        assertEquals(mapping, model.mappings().get("_doc"));
    }

    public void testBuilderWithAliases() {
        AliasMetadataModel alias = new AliasMetadataModel.Builder("test-alias").routing("routing").build();

        IndexMetadataModel model = new IndexMetadataModel.Builder("test-index").routingNumShards(1).putAlias(alias).build();

        assertEquals(1, model.aliases().size());
        assertEquals(alias, model.aliases().get("test-alias"));
    }

    public void testBuilderWithContext() {
        ContextModel context = new ContextModel("test-context", "1.0", Collections.singletonMap("key", "value"));

        IndexMetadataModel model = new IndexMetadataModel.Builder("test-index").routingNumShards(1).context(context).build();

        assertEquals(context, model.context());
    }

    public void testBuilderWithIngestionPaused() {
        IndexMetadataModel model = new IndexMetadataModel.Builder("test-index").routingNumShards(1).ingestionPaused(true).build();

        assertEquals(Boolean.TRUE, model.ingestionPaused());
    }

    public void testBuilderCopyConstructor() {
        ContextModel context = new ContextModel("test-context", "1.0", Collections.emptyMap());
        MappingMetadataModel mapping = new MappingMetadataModel(
            "_doc",
            new CompressedData(randomByteArrayOfLength(10), randomInt()),
            false
        );

        IndexMetadataModel original = new IndexMetadataModel.Builder("test-index").version(5)
            .routingNumShards(10)
            .putMapping(mapping)
            .context(context)
            .ingestionPaused(true)
            .system(true)
            .build();

        IndexMetadataModel copy = new IndexMetadataModel.Builder(original).build();

        assertEquals(original, copy);
        assertEquals(original.hashCode(), copy.hashCode());
    }

    public void testEquals() {
        IndexMetadataModel model1 = createTestItem();
        IndexMetadataModel model2 = new IndexMetadataModel.Builder(model1).build();

        assertNotSame(model1, model2);
        assertEquals(model1, model2);
        assertEquals(model1.hashCode(), model2.hashCode());
    }

    public void testNotEquals() {
        IndexMetadataModel model1 = new IndexMetadataModel.Builder("index1").routingNumShards(1).version(1).build();
        IndexMetadataModel model2 = new IndexMetadataModel.Builder("index2").routingNumShards(1).version(1).build();
        IndexMetadataModel model3 = new IndexMetadataModel.Builder("index1").routingNumShards(1).version(2).build();

        assertNotEquals(model1, model2);
        assertNotEquals(model1, model3);
    }

    public void testBuilderWithInSyncAllocationIds() {
        Set<String> ids = new HashSet<>();
        ids.add("alloc-1");
        ids.add("alloc-2");

        IndexMetadataModel model = new IndexMetadataModel.Builder("test-index").routingNumShards(1).putInSyncAllocationIds(0, ids).build();

        assertEquals(ids, model.inSyncAllocationIds().get(0));
    }

    public void testBuilderRemoveAlias() {
        AliasMetadataModel alias1 = new AliasMetadataModel.Builder("alias1").build();
        AliasMetadataModel alias2 = new AliasMetadataModel.Builder("alias2").build();

        IndexMetadataModel.Builder builder = new IndexMetadataModel.Builder("test-index").routingNumShards(1)
            .putAlias(alias1)
            .putAlias(alias2);

        assertEquals(2, builder.aliases().size());

        builder.removeAlias("alias1");
        assertEquals(1, builder.aliases().size());
        assertNull(builder.aliases().get("alias1"));
        assertNotNull(builder.aliases().get("alias2"));

        builder.removeAllAliases();
        assertTrue(builder.aliases().isEmpty());
    }

    private static IndexMetadataModel createTestItem() {
        IndexMetadataModel.Builder builder = new IndexMetadataModel.Builder(randomAlphaOfLengthBetween(3, 10));
        builder.version(randomLongBetween(1, 100));
        builder.mappingVersion(randomLongBetween(1, 100));
        builder.settingsVersion(randomLongBetween(1, 100));
        builder.aliasesVersion(randomLongBetween(1, 100));
        builder.routingNumShards(randomIntBetween(1, 10));
        builder.state((byte) randomIntBetween(0, 1));
        builder.primaryTerms(new long[] { randomLongBetween(1, 100), randomLongBetween(1, 100) });
        builder.system(randomBoolean());

        if (randomBoolean()) {
            Map<String, Object> settings = new HashMap<>();
            settings.put("index.number_of_shards", String.valueOf(randomIntBetween(1, 10)));
            settings.put("index.number_of_replicas", String.valueOf(randomIntBetween(0, 5)));
            builder.settings(new SettingsModel(settings));
        }

        if (randomBoolean()) {
            builder.putMapping(
                new MappingMetadataModel("_doc", new CompressedData(randomByteArrayOfLength(10), randomInt()), randomBoolean())
            );
        }

        if (randomBoolean()) {
            builder.putAlias(new AliasMetadataModel.Builder(randomAlphaOfLengthBetween(3, 10)).build());
        }

        if (randomBoolean()) {
            builder.context(new ContextModel(randomAlphaOfLengthBetween(3, 10), randomAlphaOfLength(3), Collections.emptyMap()));
        }

        if (randomBoolean()) {
            builder.ingestionPaused(randomBoolean());
        }

        return builder.build();
    }

    // XContent Tests

    private static CompressedData createJsonMappingSource(String type, String content) throws IOException {
        String json = "{\"" + type + "\":" + content + "}";
        return new CompressedData(json.getBytes());
    }

    public void testXContentRoundTrip() throws IOException {
        // Create settings
        Map<String, Object> settingsMap = new HashMap<>();
        settingsMap.put("index.number_of_shards", "3");
        settingsMap.put("index.number_of_replicas", "1");
        SettingsModel settings = new SettingsModel(settingsMap);

        // Create mapping with valid JSON source
        CompressedData mappingSource = createJsonMappingSource("_doc", "{\"properties\":{\"field1\":{\"type\":\"text\"}}}");
        MappingMetadataModel mapping = new MappingMetadataModel("_doc", mappingSource, false);

        // Create alias with valid JSON filter
        CompressedData filter = new CompressedData("{\"term\":{\"user\":\"kimchy\"}}".getBytes());
        AliasMetadataModel alias = new AliasMetadataModel.Builder("test-alias").filter(filter)
            .indexRouting("idx_route")
            .searchRouting("search_route")
            .writeIndex(true)
            .build();

        // Create in-sync allocation IDs
        Set<String> allocationIds = new HashSet<>();
        allocationIds.add("alloc-1");
        allocationIds.add("alloc-2");

        // Create context
        ContextModel context = new ContextModel("test-context", "1.0", Map.of("key", "value"));

        // Build the model
        IndexMetadataModel original = new IndexMetadataModel.Builder("test-index").version(10)
            .mappingVersion(5)
            .settingsVersion(3)
            .aliasesVersion(2)
            .routingNumShards(32)
            .state((byte) 0)
            .settings(settings)
            .primaryTerms(new long[] { 1, 2, 3 })
            .putMapping(mapping)
            .putAlias(alias)
            .putInSyncAllocationIds(0, allocationIds)
            .system(true)
            .context(context)
            .ingestionPaused(false)
            .build();

        // Serialize to XContent - wrap in root object since toXContent writes field name
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        original.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        byte[] bytes = BytesReference.toBytes(BytesReference.bytes(builder));

        // Parse back from XContent
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, bytes)) {
            IndexMetadataModel parsed = IndexMetadataModel.fromXContent(parser);

            assertEquals(original.index(), parsed.index());
            assertEquals(original.version(), parsed.version());
            assertEquals(original.mappingVersion(), parsed.mappingVersion());
            assertEquals(original.settingsVersion(), parsed.settingsVersion());
            assertEquals(original.aliasesVersion(), parsed.aliasesVersion());
            assertEquals(original.routingNumShards(), parsed.routingNumShards());
            assertEquals(original.state(), parsed.state());
            assertEquals(original.isSystem(), parsed.isSystem());
            assertArrayEquals(original.primaryTerms(), parsed.primaryTerms());
            assertEquals(original.settings(), parsed.settings());
            assertEquals(original.mappings().size(), parsed.mappings().size());
            assertEquals(original.aliases().size(), parsed.aliases().size());
            assertEquals(original.inSyncAllocationIds().get(0), parsed.inSyncAllocationIds().get(0));
            assertNotNull(parsed.context());
            assertEquals(original.context().name(), parsed.context().name());
            assertEquals(original.ingestionPaused(), parsed.ingestionPaused());
        }
    }

    public void testXContentRoundTripMinimal() throws IOException {
        IndexMetadataModel original = new IndexMetadataModel.Builder("minimal-index").version(1)
            .routingNumShards(1)
            .state((byte) 0)
            .build();

        // Serialize to XContent - wrap in root object since toXContent writes field name
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        original.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        byte[] bytes = BytesReference.toBytes(BytesReference.bytes(builder));

        // Parse back from XContent
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, bytes)) {
            IndexMetadataModel parsed = IndexMetadataModel.fromXContent(parser);

            assertEquals("minimal-index", parsed.index());
            assertEquals(1, parsed.version());
            assertEquals(1, parsed.routingNumShards());
            assertEquals((byte) 0, parsed.state());
            assertTrue(parsed.mappings().isEmpty());
            assertTrue(parsed.aliases().isEmpty());
            assertFalse(parsed.isSystem());
            assertNull(parsed.context());
            assertNull(parsed.ingestionPaused());
        }
    }

    public void testXContentRoundTripWithClosedState() throws IOException {
        IndexMetadataModel original = new IndexMetadataModel.Builder("closed-index").version(1)
            .routingNumShards(1)
            .state((byte) 1) // CLOSE
            .build();

        // Serialize to XContent - wrap in root object since toXContent writes field name
        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        original.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        byte[] bytes = BytesReference.toBytes(BytesReference.bytes(builder));

        // Parse back from XContent
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, bytes)) {
            IndexMetadataModel parsed = IndexMetadataModel.fromXContent(parser);

            assertEquals("closed-index", parsed.index());
            assertEquals((byte) 1, parsed.state());
        }
    }

    public void testFromXContentWithMultipleAliases() throws IOException {
        String json = "{\"multi-alias-index\":{"
            + "\"version\":1,"
            + "\"mapping_version\":1,"
            + "\"settings_version\":1,"
            + "\"aliases_version\":1,"
            + "\"routing_num_shards\":1,"
            + "\"state\":\"open\","
            + "\"settings\":{},"
            + "\"mappings\":{},"
            + "\"aliases\":{"
            + "\"alias1\":{\"index_routing\":\"r1\"},"
            + "\"alias2\":{\"search_routing\":\"r2\",\"is_write_index\":true}"
            + "},"
            + "\"primary_terms\":{},"
            + "\"in_sync_allocations\":{},"
            + "\"rollover_info\":{},"
            + "\"system\":false"
            + "}}";

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, json.getBytes())) {
            IndexMetadataModel parsed = IndexMetadataModel.fromXContent(parser);

            assertEquals("multi-alias-index", parsed.index());
            assertEquals(2, parsed.aliases().size());
            assertNotNull(parsed.aliases().get("alias1"));
            assertNotNull(parsed.aliases().get("alias2"));
            assertEquals("r1", parsed.aliases().get("alias1").indexRouting());
            assertEquals("r2", parsed.aliases().get("alias2").searchRouting());
            assertEquals(Boolean.TRUE, parsed.aliases().get("alias2").writeIndex());
        }
    }

    public void testFromXContentWithPrimaryTermsAsObject() throws IOException {
        String json = "{\"pt-index\":{"
            + "\"version\":1,"
            + "\"mapping_version\":1,"
            + "\"settings_version\":1,"
            + "\"aliases_version\":1,"
            + "\"routing_num_shards\":3,"
            + "\"state\":\"open\","
            + "\"settings\":{},"
            + "\"mappings\":{},"
            + "\"aliases\":{},"
            + "\"primary_terms\":{\"0\":5,\"1\":3,\"2\":7},"
            + "\"in_sync_allocations\":{},"
            + "\"rollover_info\":{},"
            + "\"system\":false"
            + "}}";

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, json.getBytes())) {
            IndexMetadataModel parsed = IndexMetadataModel.fromXContent(parser);

            assertEquals("pt-index", parsed.index());
            assertNotNull(parsed.primaryTerms());
            assertEquals(3, parsed.primaryTerms().length);
            assertEquals(5, parsed.primaryTerms()[0]);
            assertEquals(3, parsed.primaryTerms()[1]);
            assertEquals(7, parsed.primaryTerms()[2]);
        }
    }

    // Lambda writeTo/readFrom tests

    public void testWriteToWithLambdaWriters() throws IOException {
        IndexMetadataModel original = new IndexMetadataModel.Builder("lambda-test").version(1)
            .routingNumShards(1)
            .primaryTerms(new long[] { 1 })
            .build();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        StreamOutput out = new OutputStreamStreamOutput(baos);
        out.setVersion(Version.CURRENT);

        // Write with lambda writers that write simple string maps
        original.writeTo(out, o -> {
            o.writeVInt(1); // 1 custom entry
            o.writeString("my_key");
            // Write with length prefix for SKIPPABLE_FIELDS_VERSION
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            StreamOutput bufOut = new OutputStreamStreamOutput(buf);
            bufOut.writeString("my_value");
            bufOut.close();
            byte[] bytes = buf.toByteArray();
            o.writeVInt(bytes.length);
            o.writeBytes(bytes);
        }, o -> {
            o.writeVInt(1); // 1 rollover entry
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            StreamOutput bufOut = new OutputStreamStreamOutput(buf);
            bufOut.writeString("rollover_data");
            bufOut.close();
            byte[] bytes = buf.toByteArray();
            o.writeVInt(bytes.length);
            o.writeBytes(bytes);
        });
        out.close();

        // Read back with lambda readers
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        StreamInput in = new InputStreamStreamInput(bais);
        in.setVersion(Version.CURRENT);

        IndexMetadataModel parsed = new IndexMetadataModel(in, StreamInput::readString, StreamInput::readString, s -> "rollover_alias");

        assertEquals("lambda-test", parsed.index());
        assertNotNull(parsed.customData());
        assertEquals(1, parsed.customData().size());
        assertEquals("my_value", parsed.customData().get("my_key"));
        assertNotNull(parsed.rolloverInfos());
        assertEquals(1, parsed.rolloverInfos().size());
        assertEquals("rollover_data", parsed.rolloverInfos().get("rollover_alias"));
    }

    public void testWriteToWithNullLambdasWritesZero() throws IOException {
        IndexMetadataModel original = new IndexMetadataModel.Builder("null-lambda").version(1)
            .routingNumShards(1)
            .primaryTerms(new long[] { 1 })
            .build();

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        StreamOutput out = new OutputStreamStreamOutput(baos);
        out.setVersion(Version.CURRENT);
        original.writeTo(out, null, null);
        out.close();

        // Read back — should have 0 customData and 0 rolloverInfos
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        StreamInput in = new InputStreamStreamInput(bais);
        in.setVersion(Version.CURRENT);
        IndexMetadataModel parsed = new IndexMetadataModel(in);

        assertEquals("null-lambda", parsed.index());
        assertNull(parsed.customData());
        assertNull(parsed.rolloverInfos());
    }

    // SKIPPABLE_FIELDS_VERSION skip logic test

    public void testStreamSkipCustomDataAndRolloverInfos() throws IOException {
        IndexMetadataModel original = new IndexMetadataModel.Builder("skip-test").version(1)
            .routingNumShards(1)
            .primaryTerms(new long[] { 1 })
            .system(true)
            .build();

        // Write with lambda writers that include data
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        StreamOutput out = new OutputStreamStreamOutput(baos);
        out.setVersion(Version.CURRENT);
        original.writeTo(out, o -> {
            o.writeVInt(2); // 2 custom entries
            for (int i = 0; i < 2; i++) {
                o.writeString("key" + i);
                ByteArrayOutputStream buf = new ByteArrayOutputStream();
                StreamOutput bufOut = new OutputStreamStreamOutput(buf);
                bufOut.writeString("value" + i);
                bufOut.close();
                byte[] bytes = buf.toByteArray();
                o.writeVInt(bytes.length);
                o.writeBytes(bytes);
            }
        }, o -> {
            o.writeVInt(1); // 1 rollover entry
            ByteArrayOutputStream buf = new ByteArrayOutputStream();
            StreamOutput bufOut = new OutputStreamStreamOutput(buf);
            bufOut.writeString("rollover_data");
            bufOut.close();
            byte[] bytes = buf.toByteArray();
            o.writeVInt(bytes.length);
            o.writeBytes(bytes);
        });
        out.close();

        // Read back with null readers — should skip customData and rolloverInfos
        ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        StreamInput in = new InputStreamStreamInput(bais);
        in.setVersion(Version.CURRENT);
        IndexMetadataModel parsed = new IndexMetadataModel(in, null, null, null);

        assertEquals("skip-test", parsed.index());
        assertTrue(parsed.isSystem());
        assertNull(parsed.customData());
        assertNull(parsed.rolloverInfos());
    }

    // Lambda toXContent test

    public void testToXContentWithLambdaWriters() throws IOException {
        Map<String, Object> settingsMap = new HashMap<>();
        settingsMap.put("index.number_of_shards", "1");
        IndexMetadataModel original = new IndexMetadataModel.Builder("xcontent-lambda").version(1)
            .routingNumShards(1)
            .primaryTerms(new long[] { 1 })
            .settings(new SettingsModel(settingsMap))
            .build();

        XContentBuilder builder = JsonXContent.contentBuilder();
        builder.startObject();
        original.toXContent(builder, ToXContent.EMPTY_PARAMS, xb -> {
            // Write a custom data entry
            xb.startObject("my_custom");
            xb.field("key1", "val1");
            xb.endObject();
        }, xb -> {
            // Write a rollover info entry
            xb.startObject("my_rollover");
            xb.field("alias", "test_alias");
            xb.endObject();
        });
        builder.endObject();
        byte[] bytes = BytesReference.toBytes(BytesReference.bytes(builder));

        // Parse back with lambda parsers
        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, bytes)) {
            IndexMetadataModel parsed = IndexMetadataModel.fromXContent(parser, (p, name) -> {
                Map<String, Object> map = p.map();
                return map;
            }, (p, name) -> {
                Map<String, Object> map = p.map();
                return map;
            });

            assertEquals("xcontent-lambda", parsed.index());
            assertNotNull(parsed.customData());
            assertEquals(1, parsed.customData().size());
            @SuppressWarnings("unchecked")
            Map<String, Object> custom = (Map<String, Object>) parsed.customData().get("my_custom");
            assertEquals("val1", custom.get("key1"));
            assertNotNull(parsed.rolloverInfos());
            assertEquals(1, parsed.rolloverInfos().size());
        }
    }

    public void testFromXContentSkipsCustomDataAndRolloverInfosWhenNullParsers() throws IOException {
        // JSON with customData and rolloverInfos
        String json = "{\"skip-xcontent\":{"
            + "\"version\":1,"
            + "\"mapping_version\":1,"
            + "\"settings_version\":1,"
            + "\"aliases_version\":1,"
            + "\"routing_num_shards\":1,"
            + "\"state\":\"open\","
            + "\"settings\":{},"
            + "\"mappings\":{},"
            + "\"aliases\":{},"
            + "\"primary_terms\":{},"
            + "\"in_sync_allocations\":{},"
            + "\"rollover_info\":{\"alias1\":{\"conditions\":{},\"met_conditions\":{},\"time\":123}},"
            + "\"system\":false,"
            + "\"my_custom_data\":{\"key\":\"value\"}"
            + "}}";

        try (XContentParser parser = JsonXContent.jsonXContent.createParser(null, null, json.getBytes())) {
            // null parsers — should skip customData and rolloverInfos
            IndexMetadataModel parsed = IndexMetadataModel.fromXContent(parser);
            assertEquals("skip-xcontent", parsed.index());
            assertNull(parsed.customData());
            assertNull(parsed.rolloverInfos());
        }
    }

    // Builder customData/rolloverInfos tests

    public void testBuilderPutCustomDataAndRolloverInfo() {
        IndexMetadataModel.Builder builder = new IndexMetadataModel.Builder("builder-custom").routingNumShards(1)
            .primaryTerms(new long[] { 1 });

        builder.putCustomData("custom1", "value1");
        builder.putCustomData("custom2", "value2");
        builder.putRolloverInfo("rollover1", "info1");

        assertNotNull(builder.customData());
        assertEquals(2, builder.customData().size());
        assertEquals("value1", builder.customData().get("custom1"));
        assertNotNull(builder.rolloverInfos());
        assertEquals(1, builder.rolloverInfos().size());
        assertEquals("info1", builder.rolloverInfos().get("rollover1"));

        IndexMetadataModel model = builder.build();
        assertNotNull(model.customData());
        assertEquals(2, model.customData().size());
        assertNotNull(model.rolloverInfos());
        assertEquals(1, model.rolloverInfos().size());
    }

    // hashCode test

    public void testHashCode() {
        IndexMetadataModel model1 = new IndexMetadataModel.Builder("hash-test").version(1)
            .routingNumShards(1)
            .primaryTerms(new long[] { 1 })
            .system(true)
            .build();

        IndexMetadataModel model2 = new IndexMetadataModel.Builder("hash-test").version(1)
            .routingNumShards(1)
            .primaryTerms(new long[] { 1 })
            .system(true)
            .build();

        IndexMetadataModel model3 = new IndexMetadataModel.Builder("hash-test").version(2)
            .routingNumShards(1)
            .primaryTerms(new long[] { 1 })
            .system(true)
            .build();

        assertEquals(model1.hashCode(), model2.hashCode());
        assertNotEquals(model1.hashCode(), model3.hashCode());
    }
}
