/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.metadata.index.model;

import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.metadata.compress.CompressedData;
import org.opensearch.metadata.settings.SettingsModel;
import org.opensearch.metadata.stream.MetadataWriteable;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

import static org.hamcrest.Matchers.equalTo;

public class IndexMetadataModelTests extends OpenSearchTestCase {

    /**
     * Simple test implementation of MetadataWriteable for customData.
     */
    private static class TestCustomData implements MetadataWriteable {
        private final Map<String, String> data;

        TestCustomData(Map<String, String> data) {
            this.data = data;
        }

        TestCustomData(StreamInput in) throws IOException {
            this.data = in.readMap(StreamInput::readString, StreamInput::readString);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(data, StreamOutput::writeString, StreamOutput::writeString);
        }

        @Override
        public void writeToMetadataStream(StreamOutput out) throws IOException {
            writeTo(out);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestCustomData that = (TestCustomData) o;
            return Objects.equals(data, that.data);
        }

        @Override
        public int hashCode() {
            return Objects.hash(data);
        }

        static final MetadataWriteable.MetadataReader<TestCustomData> METADATA_READER = TestCustomData::new;
    }

    /**
     * Simple test implementation of MetadataWriteable for rolloverInfo.
     */
    private static class TestRolloverInfo implements MetadataWriteable {
        private final String alias;
        private final long time;

        TestRolloverInfo(String alias, long time) {
            this.alias = alias;
            this.time = time;
        }

        TestRolloverInfo(StreamInput in) throws IOException {
            this.alias = in.readString();
            this.time = in.readLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(alias);
            out.writeLong(time);
        }

        @Override
        public void writeToMetadataStream(StreamOutput out) throws IOException {
            writeTo(out);
        }

        public String getAlias() {
            return alias;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            TestRolloverInfo that = (TestRolloverInfo) o;
            return time == that.time && Objects.equals(alias, that.alias);
        }

        @Override
        public int hashCode() {
            return Objects.hash(alias, time);
        }

        static final MetadataWriteable.MetadataReader<TestRolloverInfo> METADATA_READER = TestRolloverInfo::new;
    }

    public void testSerialization() throws IOException {
        final IndexMetadataModel before = createTestItem();

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final StreamOutput out = new OutputStreamStreamOutput(baos);
        before.writeTo(out);
        out.close();

        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        final StreamInput in = new InputStreamStreamInput(bais);
        final IndexMetadataModel after = new IndexMetadataModel(
            in,
            TestCustomData.METADATA_READER,
            TestRolloverInfo.METADATA_READER,
            rolloverInfo -> ((TestRolloverInfo) rolloverInfo).getAlias()
        );

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
        final IndexMetadataModel after = new IndexMetadataModel(
            in,
            TestCustomData.METADATA_READER,
            TestRolloverInfo.METADATA_READER,
            rolloverInfo -> ((TestRolloverInfo) rolloverInfo).getAlias()
        );

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

        Map<String, String> customData = new HashMap<>();
        customData.put("custom_key", "custom_value");

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
            .putCustom("my_custom", new TestCustomData(customData))
            .putRolloverInfo("rollover-alias", new TestRolloverInfo("rollover-alias", System.currentTimeMillis()))
            .ingestionPaused(true)
            .build();

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final StreamOutput out = new OutputStreamStreamOutput(baos);
        before.writeTo(out);
        out.close();

        final ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
        final StreamInput in = new InputStreamStreamInput(bais);
        final IndexMetadataModel after = new IndexMetadataModel(
            in,
            TestCustomData.METADATA_READER,
            TestRolloverInfo.METADATA_READER,
            rolloverInfo -> ((TestRolloverInfo) rolloverInfo).getAlias()
        );

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
        assertThat(after.customData().size(), equalTo(1));
        assertThat(after.rolloverInfos().size(), equalTo(1));
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
        final IndexMetadataModel after = new IndexMetadataModel(
            in,
            TestCustomData.METADATA_READER,
            TestRolloverInfo.METADATA_READER,
            rolloverInfo -> ((TestRolloverInfo) rolloverInfo).getAlias()
        );

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
        final IndexMetadataModel after = new IndexMetadataModel(
            in,
            TestCustomData.METADATA_READER,
            TestRolloverInfo.METADATA_READER,
            rolloverInfo -> ((TestRolloverInfo) rolloverInfo).getAlias()
        );

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
}
