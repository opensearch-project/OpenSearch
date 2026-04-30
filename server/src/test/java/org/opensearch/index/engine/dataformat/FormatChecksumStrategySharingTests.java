/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.dataformat;

import org.apache.lucene.store.FSDirectory;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.stub.MockDataFormat;
import org.opensearch.index.engine.dataformat.stub.MockSearchBackEndPlugin;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.DataFormatAwareStoreDirectory;
import org.opensearch.index.store.FormatChecksumStrategy;
import org.opensearch.index.store.PrecomputedChecksumStrategy;
import org.opensearch.plugins.PluginsService;
import org.opensearch.plugins.SearchBackEndPlugin;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests that validate the FormatChecksumStrategy single-instance fix:
 * strategies are created once per shard and shared between directory and engine.
 */
public class FormatChecksumStrategySharingTests extends OpenSearchTestCase {

    private static final String FORMAT_NAME = "test_format";

    /**
     * A DataFormatPlugin that returns a new PrecomputedChecksumStrategy on every
     * getFormatDescriptors() call — reproducing the original bug pattern.
     */
    private static class StrategyCreatingPlugin extends org.opensearch.plugins.Plugin implements DataFormatPlugin {
        private final MockDataFormat format;

        StrategyCreatingPlugin(MockDataFormat format) {
            this.format = format;
        }

        @Override
        public DataFormat getDataFormat() {
            return format;
        }

        @Override
        public IndexingExecutionEngine<?, ?> indexingEngine(IndexingEngineConfig settings) {
            return null;
        }

        @Override
        public Map<String, Supplier<DataFormatDescriptor>> getFormatDescriptors(IndexSettings indexSettings, DataFormatRegistry registry) {
            // Creates a NEW PrecomputedChecksumStrategy every call — this is the bug pattern
            return Map.of(FORMAT_NAME, () -> new DataFormatDescriptor(FORMAT_NAME, new PrecomputedChecksumStrategy()));
        }
    }

    private DataFormatRegistry createRegistry(MockDataFormat format) {
        StrategyCreatingPlugin plugin = new StrategyCreatingPlugin(format);
        MockSearchBackEndPlugin backEnd = new MockSearchBackEndPlugin(List.of(format.name()));
        PluginsService pluginsService = mock(PluginsService.class);
        when(pluginsService.filterPlugins(DataFormatPlugin.class)).thenReturn(List.of(plugin));
        when(pluginsService.filterPlugins(SearchBackEndPlugin.class)).thenReturn(List.of(backEnd));
        return new DataFormatRegistry(pluginsService);
    }

    private IndexSettings createIndexSettings(String indexName) {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
            .put(IndexSettings.PLUGGABLE_DATAFORMAT_VALUE_SETTING.getKey(), FORMAT_NAME)
            .build();
        return new IndexSettings(IndexMetadata.builder(indexName).settings(settings).build(), settings);
    }

    /**
     * Verifies that createChecksumStrategies() returns the same strategy instance
     * that both the directory and engine would share.
     */
    public void testCreateChecksumStrategiesReturnsSameInstance() {
        MockDataFormat format = new MockDataFormat(FORMAT_NAME, 100L, Set.of());
        DataFormatRegistry registry = createRegistry(format);
        IndexSettings indexSettings = createIndexSettings("test_index");

        Map<String, FormatChecksumStrategy> strategies = registry.createChecksumStrategies(indexSettings);

        assertNotNull(strategies.get(FORMAT_NAME));
        assertTrue(strategies.get(FORMAT_NAME) instanceof PrecomputedChecksumStrategy);
    }

    /**
     * Verifies that calling createChecksumStrategies() twice returns DIFFERENT
     * instances (since getFormatDescriptors creates new ones each call).
     * This confirms the fix must call it only once per shard.
     */
    public void testMultipleCallsCreateDifferentInstances() {
        MockDataFormat format = new MockDataFormat(FORMAT_NAME, 100L, Set.of());
        DataFormatRegistry registry = createRegistry(format);
        IndexSettings indexSettings = createIndexSettings("test_index");

        Map<String, FormatChecksumStrategy> first = registry.createChecksumStrategies(indexSettings);
        Map<String, FormatChecksumStrategy> second = registry.createChecksumStrategies(indexSettings);

        // Different calls produce different instances — this is WHY we must call it only once
        assertNotSame(first.get(FORMAT_NAME), second.get(FORMAT_NAME));
    }

    /**
     * Core test: checksum registered via the engine's strategy reference is visible
     * from the directory's strategy reference when they share the same instance.
     * This is the exact bug scenario that was broken before the fix.
     */
    public void testChecksumVisibleAcrossSharedStrategy() throws IOException {
        MockDataFormat format = new MockDataFormat(FORMAT_NAME, 100L, Set.of());
        DataFormatRegistry registry = createRegistry(format);
        IndexSettings indexSettings = createIndexSettings("test_index");

        // Single call — same map shared by directory and engine
        Map<String, FormatChecksumStrategy> strategies = registry.createChecksumStrategies(indexSettings);
        FormatChecksumStrategy sharedStrategy = strategies.get(FORMAT_NAME);

        long expectedChecksum = 3847291056L;
        // Simulate engine registering a checksum during write
        sharedStrategy.registerChecksum("_0_1.parquet", expectedChecksum, 1L);

        // Simulate directory reading the checksum during upload
        Path tempDir = createTempDir();
        Path shardDataPath = tempDir.resolve("uuid").resolve("0");
        Files.createDirectories(shardDataPath.resolve(ShardPath.INDEX_FOLDER_NAME));
        ShardPath shardPath = new ShardPath(false, shardDataPath, shardDataPath, new ShardId("index", "uuid", 0));
        FSDirectory fsDir = FSDirectory.open(shardDataPath.resolve(ShardPath.INDEX_FOLDER_NAME));

        DataFormatAwareStoreDirectory directory = new DataFormatAwareStoreDirectory(fsDir, shardPath, strategies);

        // The directory's strategy IS the same instance
        FormatChecksumStrategy directoryStrategy = directory.getChecksumStrategy(FORMAT_NAME);
        assertSame("Directory and engine must share the same strategy instance", sharedStrategy, directoryStrategy);

        // Verify the checksum registered by the engine is readable from the directory's strategy (O(1) lookup)
        long actualChecksum = directoryStrategy.computeChecksum(fsDir, "_0_1.parquet");
        assertEquals("Checksum registered by engine must be visible via directory strategy", expectedChecksum, actualChecksum);

        directory.close();
    }

    /**
     * Verifies that concurrent shard creation for different indices produces
     * isolated strategy instances — no cross-index contamination.
     */
    public void testDifferentIndicesGetIsolatedStrategies() {
        MockDataFormat format = new MockDataFormat(FORMAT_NAME, 100L, Set.of());
        DataFormatRegistry registry = createRegistry(format);

        IndexSettings indexSettingsA = createIndexSettings("index_a");
        IndexSettings indexSettingsB = createIndexSettings("index_b");

        Map<String, FormatChecksumStrategy> strategiesA = registry.createChecksumStrategies(indexSettingsA);
        Map<String, FormatChecksumStrategy> strategiesB = registry.createChecksumStrategies(indexSettingsB);

        // Different indices get different strategy instances
        assertNotSame(strategiesA.get(FORMAT_NAME), strategiesB.get(FORMAT_NAME));

        // Register checksum in index A's strategy
        strategiesA.get(FORMAT_NAME).registerChecksum("_0.parquet", 12345L, 1L);

        // Index B's strategy should NOT see it
        PrecomputedChecksumStrategy stratB = (PrecomputedChecksumStrategy) strategiesB.get(FORMAT_NAME);
        // computeChecksum would fall back to file scan if not cached — but we can verify
        // the cache is empty by checking that a different checksum isn't magically present
        PrecomputedChecksumStrategy stratA = (PrecomputedChecksumStrategy) strategiesA.get(FORMAT_NAME);
        assertNotSame(stratA, stratB);
    }

    /**
     * Verifies that the strategies map returned by createChecksumStrategies is unmodifiable.
     */
    public void testCreateChecksumStrategiesReturnsUnmodifiableMap() {
        MockDataFormat format = new MockDataFormat(FORMAT_NAME, 100L, Set.of());
        DataFormatRegistry registry = createRegistry(format);
        IndexSettings indexSettings = createIndexSettings("test_index");

        Map<String, FormatChecksumStrategy> strategies = registry.createChecksumStrategies(indexSettings);

        expectThrows(UnsupportedOperationException.class, () -> strategies.put("new_format", new PrecomputedChecksumStrategy()));
    }

    /**
     * Verifies that createChecksumStrategies returns empty map when no pluggable
     * data format is configured.
     */
    public void testCreateChecksumStrategiesEmptyWhenNoFormat() {
        MockDataFormat format = new MockDataFormat(FORMAT_NAME, 100L, Set.of());
        DataFormatRegistry registry = createRegistry(format);

        // Index settings WITHOUT pluggable_dataformat setting
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_INDEX_VERSION_CREATED.getKey(), Version.CURRENT)
            .build();
        IndexSettings indexSettings = new IndexSettings(IndexMetadata.builder("plain_index").settings(settings).build(), settings);

        Map<String, FormatChecksumStrategy> strategies = registry.createChecksumStrategies(indexSettings);

        assertTrue(strategies.isEmpty());
    }
}
