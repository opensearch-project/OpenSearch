/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.metadata.remote;

import org.apache.lucene.codecs.CodecUtil;
import org.apache.lucene.store.ByteBuffersDataOutput;
import org.apache.lucene.store.ByteBuffersIndexOutput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.metadata.common.XContentContext;
import org.opensearch.metadata.index.model.AliasMetadataModel;
import org.opensearch.metadata.index.model.IndexMetadataModel;
import org.opensearch.metadata.settings.SettingsModel;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class RemoteIndexMetadataModelTests extends OpenSearchTestCase {

    private static final String CODEC_NAME = "index-metadata";
    private static final int CODEC_VERSION = 1;

    public void testDeserializeBasicIndexMetadata() throws IOException {
        // Create a simple IndexMetadataModel
        IndexMetadataModel original = createTestIndexMetadataModel("test-index");

        // Serialize to SMILE format with Lucene checksum
        byte[] serialized = serializeWithChecksum(original);

        // Deserialize using the reader
        RemoteIndexMetadataModel reader = new RemoteIndexMetadataModel();
        IndexMetadataModel deserialized = reader.deserialize(new ByteArrayInputStream(serialized));

        // Verify
        assertEquals(original.index(), deserialized.index());
        assertEquals(original.version(), deserialized.version());
        assertEquals(original.mappingVersion(), deserialized.mappingVersion());
        assertEquals(original.settingsVersion(), deserialized.settingsVersion());
        assertEquals(original.aliasesVersion(), deserialized.aliasesVersion());
        assertEquals(original.routingNumShards(), deserialized.routingNumShards());
        assertEquals(original.state(), deserialized.state());
        assertEquals(original.isSystem(), deserialized.isSystem());
    }

    public void testDeserializeWithSettings() throws IOException {
        Map<String, Object> settingsMap = new HashMap<>();
        settingsMap.put("index.number_of_shards", "5");
        settingsMap.put("index.number_of_replicas", "1");
        SettingsModel settings = new SettingsModel(settingsMap);

        IndexMetadataModel.Builder builder = new IndexMetadataModel.Builder("test-index-with-settings");
        builder.version(1);
        builder.mappingVersion(1);
        builder.settingsVersion(1);
        builder.aliasesVersion(1);
        builder.routingNumShards(5);
        builder.state((byte) 0);
        builder.settings(settings);
        builder.primaryTerms(new long[] { 1 });
        IndexMetadataModel original = builder.build();

        byte[] serialized = serializeWithChecksum(original);

        RemoteIndexMetadataModel reader = new RemoteIndexMetadataModel();
        IndexMetadataModel deserialized = reader.deserialize(new ByteArrayInputStream(serialized));

        assertEquals("5", deserialized.settings().getSettings().get("index.number_of_shards"));
        assertEquals("1", deserialized.settings().getSettings().get("index.number_of_replicas"));
    }

    public void testDeserializeWithAliases() throws IOException {
        IndexMetadataModel.Builder builder = new IndexMetadataModel.Builder("test-index-with-aliases");
        builder.version(1);
        builder.mappingVersion(1);
        builder.settingsVersion(1);
        builder.aliasesVersion(1);
        builder.routingNumShards(1);
        builder.state((byte) 0);
        builder.settings(SettingsModel.EMPTY);
        builder.primaryTerms(new long[] { 1 });

        AliasMetadataModel alias1 = new AliasMetadataModel.Builder("alias1").build();
        AliasMetadataModel alias2 = new AliasMetadataModel.Builder("alias2").indexRouting("routing1").build();
        builder.putAlias(alias1);
        builder.putAlias(alias2);

        IndexMetadataModel original = builder.build();
        byte[] serialized = serializeWithChecksum(original);

        RemoteIndexMetadataModel reader = new RemoteIndexMetadataModel();
        IndexMetadataModel deserialized = reader.deserialize(new ByteArrayInputStream(serialized));

        assertEquals(2, deserialized.aliases().size());
        assertTrue(deserialized.aliases().containsKey("alias1"));
        assertTrue(deserialized.aliases().containsKey("alias2"));
        assertEquals("routing1", deserialized.aliases().get("alias2").indexRouting());
    }

    public void testDeserializeWithInSyncAllocationIds() throws IOException {
        IndexMetadataModel.Builder builder = new IndexMetadataModel.Builder("test-index-with-allocations");
        builder.version(1);
        builder.mappingVersion(1);
        builder.settingsVersion(1);
        builder.aliasesVersion(1);
        builder.routingNumShards(2);
        builder.state((byte) 0);
        builder.settings(SettingsModel.EMPTY);
        builder.primaryTerms(new long[] { 1, 2 });

        Set<String> shard0Ids = new HashSet<>();
        shard0Ids.add("alloc-id-1");
        shard0Ids.add("alloc-id-2");
        builder.putInSyncAllocationIds(0, shard0Ids);

        Set<String> shard1Ids = new HashSet<>();
        shard1Ids.add("alloc-id-3");
        builder.putInSyncAllocationIds(1, shard1Ids);

        IndexMetadataModel original = builder.build();
        byte[] serialized = serializeWithChecksum(original);

        RemoteIndexMetadataModel reader = new RemoteIndexMetadataModel();
        IndexMetadataModel deserialized = reader.deserialize(new ByteArrayInputStream(serialized));

        assertEquals(2, deserialized.inSyncAllocationIds().size());
        assertTrue(deserialized.inSyncAllocationIds().get(0).contains("alloc-id-1"));
        assertTrue(deserialized.inSyncAllocationIds().get(0).contains("alloc-id-2"));
        assertTrue(deserialized.inSyncAllocationIds().get(1).contains("alloc-id-3"));
    }

    public void testDeserializeWithCompression() throws IOException {
        IndexMetadataModel original = createTestIndexMetadataModel("compressed-index");
        byte[] uncompressedContent = serializeToSmile(original);

        // Mock compressor
        Compressor mockCompressor = mock(Compressor.class);
        byte[] compressedContent = "compressed-data".getBytes();

        // When isCompressed is called, return true
        when(mockCompressor.isCompressed(any(BytesReference.class))).thenReturn(true);
        // When uncompress is called, return the original content
        when(mockCompressor.uncompress(any(BytesReference.class))).thenReturn(new BytesArray(uncompressedContent));

        // Create serialized data with checksum wrapping the "compressed" content
        byte[] serialized = wrapWithChecksum(compressedContent);

        RemoteIndexMetadataModel reader = new RemoteIndexMetadataModel(mockCompressor);
        IndexMetadataModel deserialized = reader.deserialize(new ByteArrayInputStream(serialized));

        assertEquals(original.index(), deserialized.index());
    }

    public void testDeserializeWithoutCompression() throws IOException {
        IndexMetadataModel original = createTestIndexMetadataModel("uncompressed-index");
        byte[] serialized = serializeWithChecksum(original);

        // Mock compressor that says data is not compressed
        Compressor mockCompressor = mock(Compressor.class);
        when(mockCompressor.isCompressed(any(BytesReference.class))).thenReturn(false);

        RemoteIndexMetadataModel reader = new RemoteIndexMetadataModel(mockCompressor);
        IndexMetadataModel deserialized = reader.deserialize(new ByteArrayInputStream(serialized));

        assertEquals(original.index(), deserialized.index());
    }

    public void testSerializeThrowsUnsupportedOperationException() {
        RemoteIndexMetadataModel reader = new RemoteIndexMetadataModel();
        expectThrows(UnsupportedOperationException.class, reader::serialize);
    }

    public void testDeserializeCorruptedData() {
        byte[] corruptedData = new byte[] { 0, 1, 2, 3, 4, 5 };
        RemoteIndexMetadataModel reader = new RemoteIndexMetadataModel();

        expectThrows(CorruptMetadataException.class, () -> reader.deserialize(new ByteArrayInputStream(corruptedData)));
    }

    public void testDeserializeEmptyData() {
        byte[] emptyData = new byte[0];
        RemoteIndexMetadataModel reader = new RemoteIndexMetadataModel();

        expectThrows(CorruptMetadataException.class, () -> reader.deserialize(new ByteArrayInputStream(emptyData)));
    }

    public void testDeserializeWithClosedState() throws IOException {
        IndexMetadataModel.Builder builder = new IndexMetadataModel.Builder("closed-index");
        builder.version(1);
        builder.mappingVersion(1);
        builder.settingsVersion(1);
        builder.aliasesVersion(1);
        builder.routingNumShards(1);
        builder.state((byte) 1); // CLOSE state
        builder.settings(SettingsModel.EMPTY);
        builder.primaryTerms(new long[] { 1 });
        IndexMetadataModel original = builder.build();

        byte[] serialized = serializeWithChecksum(original);

        RemoteIndexMetadataModel reader = new RemoteIndexMetadataModel();
        IndexMetadataModel deserialized = reader.deserialize(new ByteArrayInputStream(serialized));

        assertEquals((byte) 1, deserialized.state());
    }

    public void testDeserializeSystemIndex() throws IOException {
        IndexMetadataModel.Builder builder = new IndexMetadataModel.Builder(".system-index");
        builder.version(1);
        builder.mappingVersion(1);
        builder.settingsVersion(1);
        builder.aliasesVersion(1);
        builder.routingNumShards(1);
        builder.state((byte) 0);
        builder.settings(SettingsModel.EMPTY);
        builder.primaryTerms(new long[] { 1 });
        builder.system(true);
        IndexMetadataModel original = builder.build();

        byte[] serialized = serializeWithChecksum(original);

        RemoteIndexMetadataModel reader = new RemoteIndexMetadataModel();
        IndexMetadataModel deserialized = reader.deserialize(new ByteArrayInputStream(serialized));

        assertTrue(deserialized.isSystem());
    }

    // Helper methods

    private IndexMetadataModel createTestIndexMetadataModel(String indexName) {
        IndexMetadataModel.Builder builder = new IndexMetadataModel.Builder(indexName);
        builder.version(1);
        builder.mappingVersion(1);
        builder.settingsVersion(1);
        builder.aliasesVersion(1);
        builder.routingNumShards(1);
        builder.state((byte) 0);
        builder.settings(SettingsModel.EMPTY);
        builder.primaryTerms(new long[] { 1 });
        return builder.build();
    }

    private byte[] serializeWithChecksum(IndexMetadataModel model) throws IOException {
        byte[] smileContent = serializeToSmile(model);
        return wrapWithChecksum(smileContent);
    }

    private byte[] serializeToSmile(IndexMetadataModel model) throws IOException {
        ToXContent.Params gatewayParams = new ToXContent.MapParams(
            Collections.singletonMap(XContentContext.PARAM_KEY, XContentContext.GATEWAY.name())
        );
        try (XContentBuilder builder = XContentType.SMILE.contentBuilder()) {
            builder.startObject();
            model.toXContent(builder, gatewayParams);
            builder.endObject();
            return BytesReference.toBytes(BytesReference.bytes(builder));
        }
    }

    private byte[] wrapWithChecksum(byte[] content) throws IOException {
        ByteBuffersDataOutput dataOutput = new ByteBuffersDataOutput();
        try (IndexOutput indexOutput = new ByteBuffersIndexOutput(dataOutput, "test", "test")) {
            CodecUtil.writeHeader(indexOutput, CODEC_NAME, CODEC_VERSION);
            indexOutput.writeBytes(content, content.length);
            CodecUtil.writeFooter(indexOutput);
        }
        return dataOutput.toArrayCopy();
    }
}
