/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.store.subdirectory;

import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.StringField;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.engine.EngineException;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.index.engine.InternalEngine;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 3)
public class SubdirectoryAwareRecoveryTests extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(SubdirectoryStorePlugin.class, TestEnginePlugin.class);
    }

    @Override
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        return super.getMockPlugins().stream()
            .filter(plugin -> !plugin.getName().contains("MockEngineFactoryPlugin"))
            .collect(java.util.stream.Collectors.toList());
    }

    public void testSubdirectoryAwareRecovery() throws Exception {
        // Create index with custom store and engine
        Settings indexSettings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.store.factory", "subdirectory_store")
            .put(TestEnginePlugin.TEST_ENGINE_INDEX_SETTING.getKey(), true)
            .build();

        prepareCreate("test_index").setSettings(indexSettings).get();
        ensureGreen("test_index");

        // Index documents to create content
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test_index").setId(Integer.toString(i)).setSource("field", "value" + i).get();
        }

        client().admin().indices().prepareFlush("test_index").get();
        client().admin().indices().prepareRefresh("test_index").get();

        // Add replica to trigger recovery
        client().admin()
            .indices()
            .prepareUpdateSettings("test_index")
            .setSettings(Settings.builder().put("index.number_of_replicas", 2))
            .get();

        // Verify recovery completes successfully
        ensureGreen("test_index");

        // Verify subdirectory files were copied to both primary and replica
        verifySubdirectoryFilesOnAllNodes("test_index", 3);
    }

    private void verifySubdirectoryFilesOnAllNodes(String indexName, int expectedCount) throws Exception {
        Map<String, Set<String>> nodeFiles = new HashMap<>();

        for (String nodeName : internalCluster().getNodeNames()) {

            IndicesService indicesService = internalCluster().getInstance(IndicesService.class, nodeName);
            IndexService indexService = indicesService.indexService(resolveIndex(indexName));
            if (indexService == null) {
                continue; // Index not on this node
            }
            IndexShard shard = indexService.getShard(0);
            Path subdirectoryPath = shard.shardPath().getDataPath().resolve(TestEngine.SUBDIRECTORY_NAME);

            if (Files.exists(subdirectoryPath)) {
                try (Directory directory = FSDirectory.open(subdirectoryPath)) {
                    SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(directory);
                    Collection<String> segmentFiles = segmentInfos.files(true);
                    if (!segmentFiles.isEmpty()) {
                        nodeFiles.put(nodeName, new HashSet<>(segmentFiles));
                    }
                } catch (IOException e) {
                    // corrupt index or no commit files, skip this node
                }
            }
        }

        assertEquals(
            "Expected " + expectedCount + " nodes with subdirectory files, found: " + nodeFiles.keySet(),
            expectedCount,
            nodeFiles.size()
        );

        // Verify all nodes have identical files
        if (nodeFiles.size() > 1) {
            Set<String> referenceFiles = nodeFiles.values().iterator().next();
            for (Map.Entry<String, Set<String>> entry : nodeFiles.entrySet()) {
                assertEquals("Node " + entry.getKey() + " should have identical files to other nodes", referenceFiles, entry.getValue());
            }
        }
    }

    /**
     * Plugin that provides a custom engine for testing subdirectory recovery
     */
    public static class TestEnginePlugin extends Plugin implements EnginePlugin {

        static final Setting<Boolean> TEST_ENGINE_INDEX_SETTING = Setting.boolSetting(
            "index.use_test_engine",
            false,
            Setting.Property.IndexScope
        );

        @Override
        public List<Setting<?>> getSettings() {
            return Collections.singletonList(TEST_ENGINE_INDEX_SETTING);
        }

        @Override
        public Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
            if (TEST_ENGINE_INDEX_SETTING.get(indexSettings.getSettings())) {
                return Optional.of(new TestEngineFactory());
            }
            return Optional.empty();
        }
    }

    /**
     * Factory for creating TestEngine instances
     */
    static class TestEngineFactory implements EngineFactory {
        @Override
        public Engine newReadWriteEngine(EngineConfig config) {
            try {
                return new TestEngine(config);
            } catch (IOException e) {
                throw new EngineException(config.getShardId(), "Failed to create test engine", e);
            }
        }
    }

    /**
     * Test engine that extends InternalEngine and creates a proper Lucene index in a subdirectory
     */
    static class TestEngine extends InternalEngine {

        static final String SUBDIRECTORY_NAME = "test_subdirectory";

        private final Path subdirectoryPath;
        private final Directory subdirectoryDirectory;
        private final IndexWriter subdirectoryWriter;
        private final EngineConfig engineConfig;

        TestEngine(EngineConfig config) throws IOException {
            super(config);
            this.engineConfig = config;

            // Set up subdirectory path and writer
            Path shardPath = config.getStore().shardPath().getDataPath();
            subdirectoryPath = shardPath.resolve(SUBDIRECTORY_NAME);
            Files.createDirectories(subdirectoryPath);
            subdirectoryDirectory = FSDirectory.open(subdirectoryPath);
            IndexWriterConfig writerConfig = new IndexWriterConfig();
            writerConfig.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
            subdirectoryWriter = new IndexWriter(subdirectoryDirectory, writerConfig);
        }

        @Override
        public IndexResult index(Index index) throws IOException {
            // First, index the document normally
            IndexResult result = super.index(index);

            // Only add to subdirectory if is a primary shard
            if (result.getResultType() == Engine.Result.Type.SUCCESS && engineConfig.getStartedPrimarySupplier().getAsBoolean()) {
                addDocumentToSubdirectory(index);
            }
            return result;
        }

        private void addDocumentToSubdirectory(Index index) throws IOException {
            Document doc = new Document();
            doc.add(new StringField("source_id", index.id(), Field.Store.YES));
            subdirectoryWriter.addDocument(doc);
        }

        @Override
        public void flush(boolean force, boolean waitIfOngoing) throws EngineException {
            // First flush the main engine
            super.flush(force, waitIfOngoing);
            // Then commit the subdirectory
            try {
                subdirectoryWriter.commit();
            } catch (IOException e) {
                throw new EngineException(shardId, "Failed to commit subdirectory during flush", e);
            }
        }

        @Override
        public void close() throws IOException {
            subdirectoryWriter.close();
            subdirectoryDirectory.close();
            super.close();
        }

        @Override
        public GatedCloseable<IndexCommit> acquireLastIndexCommit(boolean flushFirst) throws EngineException {
            if (flushFirst) {
                flush(false, true);
            }
            try {
                GatedCloseable<IndexCommit> realCommit = super.acquireLastIndexCommit(false);
                IndexCommit originalCommit = realCommit.get();
                TestIndexCommit testCommit = new TestIndexCommit(originalCommit, subdirectoryPath);
                realCommit.close();
                return new GatedCloseable<>(testCommit, () -> {});
            } catch (Exception e) {
                throw new EngineException(shardId, "Failed to acquire last index commit", e);
            }
        }

        @Override
        public GatedCloseable<IndexCommit> acquireSafeIndexCommit() throws EngineException {
            try {
                GatedCloseable<IndexCommit> realCommit = super.acquireSafeIndexCommit();
                IndexCommit originalCommit = realCommit.get();
                TestIndexCommit testCommit = new TestIndexCommit(originalCommit, subdirectoryPath);
                realCommit.close();
                return new GatedCloseable<>(testCommit, () -> {});
            } catch (Exception e) {
                throw new EngineException(shardId, "Failed to acquire safe index commit", e);
            }
        }
    }

    /**
     * Custom IndexCommit that includes subdirectory files in recovery
     */
    static class TestIndexCommit extends IndexCommit {

        private final IndexCommit delegate;
        private final Path subdirectoryPath;

        TestIndexCommit(IndexCommit delegate, Path subdirectoryPath) {
            this.delegate = delegate;
            this.subdirectoryPath = subdirectoryPath;
        }

        @Override
        public String getSegmentsFileName() {
            return delegate.getSegmentsFileName();
        }

        @Override
        public Collection<String> getFileNames() throws IOException {
            Set<String> allFiles = new HashSet<>(delegate.getFileNames());

            if (Files.exists(subdirectoryPath)) {
                try (Directory directory = FSDirectory.open(subdirectoryPath)) {
                    SegmentInfos segmentInfos = SegmentInfos.readLatestCommit(directory);
                    Collection<String> segmentFiles = segmentInfos.files(true);

                    for (String fileName : segmentFiles) {
                        String relativePath = Path.of(TestEngine.SUBDIRECTORY_NAME, fileName).toString();
                        allFiles.add(relativePath);
                    }
                }
            }
            return allFiles;
        }

        @Override
        public Directory getDirectory() {
            return delegate.getDirectory();
        }

        @Override
        public void delete() {
            delegate.delete();
        }

        @Override
        public int getSegmentCount() {
            return delegate.getSegmentCount();
        }

        @Override
        public long getGeneration() {
            return delegate.getGeneration();
        }

        @Override
        public Map<String, String> getUserData() throws IOException {
            return delegate.getUserData();
        }

        @Override
        public boolean isDeleted() {
            return delegate.isDeleted();
        }
    }
}
