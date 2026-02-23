/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gateway.remote.model;

import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.Context;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.compress.DeflateCompressor;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.compress.NoneCompressor;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.indices.IndicesModule;
import org.opensearch.metadata.index.model.IndexMetadataModel;
import org.opensearch.metadata.remote.CorruptMetadataException;
import org.opensearch.metadata.remote.RemoteIndexMetadataModel;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import static org.opensearch.Version.MASK;

/**
 * Integration tests for {@link RemoteIndexMetadataModel}.
 * <p>
 * These tests verify the round-trip serialization/deserialization between
 * the server module's {@link RemoteIndexMetadata} and the libs/metadata
 * module's {@link RemoteIndexMetadataModel}.
 * <p>
 * The deserialized {@link IndexMetadataModel} is verified directly for core field equivalence.
 */
public class RemoteIndexMetadataModelIntegrationTests extends OpenSearchTestCase {

    private static final String CLUSTER_UUID = "test-cluster-uuid";

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return new NamedXContentRegistry(IndicesModule.getNamedXContents());
    }

    /**
     * Test round-trip: serialize with RemoteIndexMetadata, deserialize with RemoteIndexMetadataModel.
     */
    public void testRoundTripWithoutCompression() throws IOException {
        IndexMetadata original = createTestIndexMetadata("test-index");

        Compressor compressor = new NoneCompressor();
        byte[] serializedBytes = serializeWithRemoteIndexMetadata(original, compressor);

        RemoteIndexMetadataModel reader = new RemoteIndexMetadataModel(compressor);
        IndexMetadataModel model = reader.deserialize(new ByteArrayInputStream(serializedBytes));

        assertModelMatchesIndexMetadata(original, model);
    }

    /**
     * Test round-trip with compression enabled.
     */
    public void testRoundTripWithCompression() throws IOException {
        IndexMetadata original = createTestIndexMetadata("compressed-index");

        Compressor compressor = new DeflateCompressor();
        byte[] serializedBytes = serializeWithRemoteIndexMetadata(original, compressor);

        RemoteIndexMetadataModel reader = new RemoteIndexMetadataModel(compressor);
        IndexMetadataModel model = reader.deserialize(new ByteArrayInputStream(serializedBytes));

        assertModelMatchesIndexMetadata(original, model);
    }

    /**
     * Test round-trip with aliases.
     */
    public void testRoundTripWithAliases() throws IOException {
        IndexMetadata original = IndexMetadata.builder("alias-test-index")
            .settings(createDefaultSettings())
            .putAlias(AliasMetadata.builder("alias1").writeIndex(true).build())
            .putAlias(AliasMetadata.builder("alias2").indexRouting("routing1").searchRouting("routing2").build())
            .putAlias(AliasMetadata.builder("alias3").filter("{\"term\":{\"field\":\"value\"}}").build())
            .build();

        Compressor compressor = new NoneCompressor();
        byte[] serializedBytes = serializeWithRemoteIndexMetadata(original, compressor);

        RemoteIndexMetadataModel reader = new RemoteIndexMetadataModel(compressor);
        IndexMetadataModel model = reader.deserialize(new ByteArrayInputStream(serializedBytes));

        assertEquals(3, model.aliases().size());
        assertTrue(model.aliases().containsKey("alias1"));
        assertTrue(model.aliases().containsKey("alias2"));
        assertTrue(model.aliases().containsKey("alias3"));
        assertTrue(model.aliases().get("alias1").writeIndex());
        assertEquals("routing1", model.aliases().get("alias2").indexRouting());
        assertEquals("routing2", model.aliases().get("alias2").searchRouting());
    }

    /**
     * Test round-trip with context.
     */
    public void testRoundTripWithContext() throws IOException {
        IndexMetadata original = IndexMetadata.builder("context-test-index")
            .settings(createDefaultSettings())
            .context(new Context("test-context"))
            .build();

        Compressor compressor = new NoneCompressor();
        byte[] serializedBytes = serializeWithRemoteIndexMetadata(original, compressor);

        RemoteIndexMetadataModel reader = new RemoteIndexMetadataModel(compressor);
        IndexMetadataModel model = reader.deserialize(new ByteArrayInputStream(serializedBytes));

        assertNotNull(model.context());
        assertEquals("test-context", model.context().name());
    }

    /**
     * Test round-trip with system index.
     */
    public void testRoundTripWithSystemIndex() throws IOException {
        IndexMetadata original = IndexMetadata.builder(".system-index").settings(createDefaultSettings()).system(true).build();

        Compressor compressor = new NoneCompressor();
        byte[] serializedBytes = serializeWithRemoteIndexMetadata(original, compressor);

        RemoteIndexMetadataModel reader = new RemoteIndexMetadataModel(compressor);
        IndexMetadataModel model = reader.deserialize(new ByteArrayInputStream(serializedBytes));

        assertTrue(model.isSystem());
    }

    /**
     * Test corruption detection with corrupted checksum bytes.
     */
    public void testCorruptionDetection() throws IOException {
        IndexMetadata original = createTestIndexMetadata("corruption-test-index");

        Compressor compressor = new NoneCompressor();
        byte[] serializedBytes = serializeWithRemoteIndexMetadata(original, compressor);

        // Corrupt the data (modify a byte in the middle)
        if (serializedBytes.length > 50) {
            serializedBytes[50] = (byte) (serializedBytes[50] ^ 0xFF);
        }

        RemoteIndexMetadataModel reader = new RemoteIndexMetadataModel(compressor);
        expectThrows(CorruptMetadataException.class, () -> { reader.deserialize(new ByteArrayInputStream(serializedBytes)); });
    }

    /**
     * Test with closed index state.
     */
    public void testRoundTripWithClosedState() throws IOException {
        IndexMetadata original = IndexMetadata.builder("closed-index")
            .settings(createDefaultSettings())
            .state(IndexMetadata.State.CLOSE)
            .build();

        Compressor compressor = new NoneCompressor();
        byte[] serializedBytes = serializeWithRemoteIndexMetadata(original, compressor);

        RemoteIndexMetadataModel reader = new RemoteIndexMetadataModel(compressor);
        IndexMetadataModel model = reader.deserialize(new ByteArrayInputStream(serializedBytes));

        assertEquals((byte) 1, model.state()); // CLOSE = 1
    }

    /**
     * Test with multiple shards and primary terms.
     */
    public void testRoundTripWithMultipleShards() throws IOException {
        int numShards = 5;
        IndexMetadata original = IndexMetadata.builder("multi-shard-index")
            .settings(
                Settings.builder()
                    .put("index.version.created", 1 ^ MASK)
                    .put("index.number_of_shards", numShards)
                    .put("index.number_of_replicas", 2)
                    .build()
            )
            .setRoutingNumShards(numShards * 2)
            .build();

        Compressor compressor = new NoneCompressor();
        byte[] serializedBytes = serializeWithRemoteIndexMetadata(original, compressor);

        RemoteIndexMetadataModel reader = new RemoteIndexMetadataModel(compressor);
        IndexMetadataModel model = reader.deserialize(new ByteArrayInputStream(serializedBytes));

        assertEquals(numShards * 2, model.routingNumShards());
        assertEquals(numShards, model.primaryTerms().length);
    }

    // Helper methods

    private byte[] serializeWithRemoteIndexMetadata(IndexMetadata indexMetadata, Compressor compressor) throws IOException {
        RemoteIndexMetadata remoteIndexMetadata = new RemoteIndexMetadata(
            indexMetadata,
            CLUSTER_UUID,
            compressor,
            xContentRegistry(),
            null,
            null,
            null
        );

        try (InputStream is = remoteIndexMetadata.serialize()) {
            return is.readAllBytes();
        }
    }

    private IndexMetadata createTestIndexMetadata(String indexName) {
        return IndexMetadata.builder(indexName)
            .settings(createDefaultSettings())
            .version(5L)
            .mappingVersion(2L)
            .settingsVersion(3L)
            .aliasesVersion(4L)
            .setRoutingNumShards(2)
            .primaryTerm(0, 10L)
            .build();
    }

    private Settings createDefaultSettings() {
        return Settings.builder()
            .put("index.version.created", 1 ^ MASK)
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .build();
    }

    private void assertModelMatchesIndexMetadata(IndexMetadata expected, IndexMetadataModel actual) {
        assertEquals(expected.getIndex().getName(), actual.index());
        assertEquals(expected.getVersion(), actual.version());
        assertEquals(expected.getMappingVersion(), actual.mappingVersion());
        assertEquals(expected.getSettingsVersion(), actual.settingsVersion());
        assertEquals(expected.getAliasesVersion(), actual.aliasesVersion());
        assertEquals(expected.getRoutingNumShards(), actual.routingNumShards());
        assertEquals(expected.getState().ordinal(), actual.state());
        assertEquals(expected.isSystem(), actual.isSystem());
    }
}
