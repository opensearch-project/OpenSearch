/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.SafeCommitInfo;
import org.opensearch.index.engine.dataformat.DataFormatPlugin;
import org.opensearch.index.engine.dataformat.RefreshInput;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.commit.CommitterSettings;
import org.opensearch.index.engine.exec.coord.CatalogSnapshotManager;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for {@link CompositeIndexingExecutionEngine}.
 */
public class CompositeIndexingExecutionEngineTests extends OpenSearchTestCase {

    public void testConstructorWithPrimaryOnly() {
        CompositeIndexingExecutionEngine engine = CompositeTestHelper.createStubEngine("lucene");
        assertNotNull(engine.getPrimaryDelegate());
        assertTrue(engine.getSecondaryDelegates().isEmpty());
        assertEquals("composite", engine.getDataFormat().name());
    }

    public void testConstructorWithPrimaryAndSecondary() {
        CompositeIndexingExecutionEngine engine = CompositeTestHelper.createStubEngine("lucene", "parquet");
        assertNotNull(engine.getPrimaryDelegate());
        assertEquals(1, engine.getSecondaryDelegates().size());
        assertEquals("parquet", engine.getSecondaryDelegates().iterator().next().getDataFormat().name());
    }

    public void testConstructorWithMultipleSecondaries() {
        CompositeIndexingExecutionEngine engine = CompositeTestHelper.createStubEngine("lucene", "parquet", "arrow");
        assertEquals(2, engine.getSecondaryDelegates().size());
    }

    public void testConstructorThrowsWhenPrimaryFormatNotRegistered() {
        Map<String, DataFormatPlugin> plugins = new HashMap<>();
        plugins.put("lucene", CompositeTestHelper.stubPlugin("lucene", 1));

        IndexSettings indexSettings = createIndexSettings("parquet");
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new CompositeIndexingExecutionEngine(plugins, indexSettings, null, null, new CompositeTestHelper.StubCommitter())
        );
        assertTrue(ex.getMessage().contains("parquet"));
    }

    public void testConstructorThrowsWhenSecondaryFormatNotRegistered() {
        Map<String, DataFormatPlugin> plugins = new HashMap<>();
        plugins.put("lucene", CompositeTestHelper.stubPlugin("lucene", 1));

        Settings settings = Settings.builder()
            .put("index.composite.primary_data_format", "lucene")
            .putList("index.composite.secondary_data_formats", "parquet")
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("test-index").settings(settings).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new CompositeIndexingExecutionEngine(plugins, indexSettings, null, null, new CompositeTestHelper.StubCommitter())
        );
        assertTrue(ex.getMessage().contains("parquet"));
    }

    public void testConstructorRejectsNullDataFormatPlugins() {
        IndexSettings indexSettings = createIndexSettings("lucene");
        expectThrows(
            NullPointerException.class,
            () -> new CompositeIndexingExecutionEngine(null, indexSettings, null, null, new CompositeTestHelper.StubCommitter())
        );
    }

    public void testConstructorRejectsNullIndexSettings() {
        Map<String, DataFormatPlugin> plugins = Map.of("lucene", CompositeTestHelper.stubPlugin("lucene", 1));
        expectThrows(
            NullPointerException.class,
            () -> new CompositeIndexingExecutionEngine(plugins, null, null, null, new CompositeTestHelper.StubCommitter())
        );
    }

    public void testValidateFormatsRegisteredAcceptsValidConfig() {
        Map<String, DataFormatPlugin> plugins = new HashMap<>();
        plugins.put("lucene", CompositeTestHelper.stubPlugin("lucene", 1));
        plugins.put("parquet", CompositeTestHelper.stubPlugin("parquet", 2));

        CompositeIndexingExecutionEngine.validateFormatsRegistered(plugins, "lucene", List.of("parquet"));
    }

    public void testValidateFormatsRegisteredRejectsMissingPrimary() {
        Map<String, DataFormatPlugin> plugins = new HashMap<>();
        plugins.put("lucene", CompositeTestHelper.stubPlugin("lucene", 1));

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> CompositeIndexingExecutionEngine.validateFormatsRegistered(plugins, "parquet", List.of())
        );
        assertTrue(ex.getMessage().contains("parquet"));
    }

    public void testValidateFormatsRegisteredRejectsMissingSecondary() {
        Map<String, DataFormatPlugin> plugins = new HashMap<>();
        plugins.put("lucene", CompositeTestHelper.stubPlugin("lucene", 1));

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> CompositeIndexingExecutionEngine.validateFormatsRegistered(plugins, "lucene", List.of("parquet"))
        );
        assertTrue(ex.getMessage().contains("parquet"));
    }

    public void testValidateFormatsRegisteredRejectsSecondaryEqualToPrimary() {
        Map<String, DataFormatPlugin> plugins = new HashMap<>();
        plugins.put("lucene", CompositeTestHelper.stubPlugin("lucene", 1));

        IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> CompositeIndexingExecutionEngine.validateFormatsRegistered(plugins, "lucene", List.of("lucene"))
        );
        assertTrue(ex.getMessage().contains("same as primary"));
    }

    public void testCreateWriterReturnsCompositeWriter() throws IOException {
        CompositeIndexingExecutionEngine engine = CompositeTestHelper.createStubEngine("lucene");
        CompositeWriter writer = (CompositeWriter) engine.createWriter(42L);
        assertNotNull(writer);
        assertEquals(42L, writer.getWriterGeneration());
        writer.close();
    }

    public void testGetMergerDelegatesToPrimary() {
        CompositeIndexingExecutionEngine engine = CompositeTestHelper.createStubEngine("lucene");
        assertNull(engine.getMerger());
    }

    public void testGetNativeBytesUsedSumsAllEngines() {
        CompositeIndexingExecutionEngine engine = CompositeTestHelper.createStubEngine("lucene", "parquet");
        assertEquals(0L, engine.getNativeBytesUsed());
    }

    public void testGetDataFormatReturnsCompositeDataFormat() {
        CompositeIndexingExecutionEngine engine = CompositeTestHelper.createStubEngine("lucene", "parquet");
        CompositeDataFormat format = engine.getDataFormat();
        assertNotNull(format);
        assertEquals("composite", format.name());
        assertEquals(2, format.getDataFormats().size());
    }

    public void testNewDocumentInputReturnsCompositeDocumentInput() {
        CompositeIndexingExecutionEngine engine = CompositeTestHelper.createStubEngine("lucene", "parquet");
        CompositeDocumentInput input = engine.newDocumentInput();
        assertNotNull(input);
        assertNotNull(input.getPrimaryInput());
        assertEquals(1, input.getSecondaryInputs().size());
        input.close();
    }

    public void testDeleteFilesDoesNotThrow() throws Exception {
        CompositeIndexingExecutionEngine engine = CompositeTestHelper.createStubEngine("lucene", "parquet");
        engine.deleteFiles(Map.of());
    }

    // --- Task 8.5: Property test — Committer is required ---

    /**
     * Property 2: Committer is required.
     * Attempting to construct CompositeIndexingExecutionEngine with a null Committer
     * must throw IllegalStateException.
     *
     * Validates: Requirements 3.2
     */
    public void testConstructorThrowsWhenCommitterNull() {
        Map<String, DataFormatPlugin> plugins = new HashMap<>();
        plugins.put("lucene", CompositeTestHelper.stubPlugin("lucene", 1));
        IndexSettings indexSettings = createIndexSettings("lucene");

        IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> new CompositeIndexingExecutionEngine(plugins, indexSettings, null, null, null)
        );
        assertTrue(ex.getMessage().contains("Committer must not be null"));
    }

    // --- Task 8.6: Property test — Refresh never calls Committer methods ---

    /**
     * Property 5: Refresh never calls Committer methods.
     * Running refresh() on the composite engine must not invoke commit() on the Committer.
     *
     * Validates: Requirements 3.6
     */
    public void testRefreshNeverCallsCommitterMethods() throws IOException {
        TrackingCommitter tracking = new TrackingCommitter();
        Map<String, DataFormatPlugin> plugins = new HashMap<>();
        plugins.put("lucene", CompositeTestHelper.stubPlugin("lucene", 1));
        IndexSettings indexSettings = createIndexSettings("lucene");

        CompositeIndexingExecutionEngine engine = new CompositeIndexingExecutionEngine(plugins, indexSettings, null, null, tracking);

        // Reset tracking after construction (init is called during construction)
        tracking.commitCalled = false;

        RefreshInput refreshInput = RefreshInput.builder().build();
        engine.refresh(refreshInput);

        assertFalse("commit() must not be called during refresh", tracking.commitCalled);
    }

    // --- Task 8.7: Unit tests for flush and Committer lifecycle ---

    public void testInitCalledDuringConstruction() {
        CompositeTestHelper.StubCommitter stub = new CompositeTestHelper.StubCommitter();
        Map<String, DataFormatPlugin> plugins = new HashMap<>();
        plugins.put("lucene", CompositeTestHelper.stubPlugin("lucene", 1));
        IndexSettings indexSettings = createIndexSettings("lucene");

        new CompositeIndexingExecutionEngine(plugins, indexSettings, null, null, stub);
        assertTrue("init() must be called during construction", stub.initCalled);
    }

    public void testCloseCalledDuringShutdown() {
        CompositeTestHelper.StubCommitter stub = new CompositeTestHelper.StubCommitter();
        Map<String, DataFormatPlugin> plugins = new HashMap<>();
        plugins.put("lucene", CompositeTestHelper.stubPlugin("lucene", 1));
        IndexSettings indexSettings = createIndexSettings("lucene");

        CompositeIndexingExecutionEngine engine = new CompositeIndexingExecutionEngine(plugins, indexSettings, null, null, stub);
        engine.close();
        assertTrue("close() must be called during shutdown", stub.closeCalled);
    }

    public void testInitFailurePreventsConstruction() {
        Committer failingInit = new Committer() {
            @Override
            public void init(CommitterSettings settings) throws IOException {
                throw new IOException("init failed");
            }

            @Override
            public void commit(Map<String, String> commitData) {}

            @Override
            public void close() {}

            @Override
            public Map<String, String> getLastCommittedData() {
                return Map.of();
            }

            @Override
            public CommitStats getCommitStats() {
                return null;
            }

            @Override
            public SafeCommitInfo getSafeCommitInfo() {
                return SafeCommitInfo.EMPTY;
            }
        };

        Map<String, DataFormatPlugin> plugins = new HashMap<>();
        plugins.put("lucene", CompositeTestHelper.stubPlugin("lucene", 1));
        IndexSettings indexSettings = createIndexSettings("lucene");

        RuntimeException ex = expectThrows(
            RuntimeException.class,
            () -> new CompositeIndexingExecutionEngine(plugins, indexSettings, null, null, failingInit)
        );
        assertTrue(ex.getMessage().contains("Failed to initialize committer"));
    }

    public void testCloseFailureIsLoggedAndShutdownContinues() {
        Committer failingClose = new Committer() {
            @Override
            public void init(CommitterSettings settings) {}

            @Override
            public void commit(Map<String, String> commitData) {}

            @Override
            public void close() throws IOException {
                throw new IOException("close failed");
            }

            @Override
            public Map<String, String> getLastCommittedData() {
                return Map.of();
            }

            @Override
            public CommitStats getCommitStats() {
                return null;
            }

            @Override
            public SafeCommitInfo getSafeCommitInfo() {
                return SafeCommitInfo.EMPTY;
            }
        };

        Map<String, DataFormatPlugin> plugins = new HashMap<>();
        plugins.put("lucene", CompositeTestHelper.stubPlugin("lucene", 1));
        IndexSettings indexSettings = createIndexSettings("lucene");

        CompositeIndexingExecutionEngine engine = new CompositeIndexingExecutionEngine(plugins, indexSettings, null, null, failingClose);

        // close() should not throw — it logs the error and continues
        engine.close();
    }

    public void testFlushCallsCommitterCommit() throws IOException {
        TrackingCommitter tracking = new TrackingCommitter();
        Map<String, DataFormatPlugin> plugins = new HashMap<>();
        plugins.put("lucene", CompositeTestHelper.stubPlugin("lucene", 1));
        IndexSettings indexSettings = createIndexSettings("lucene");

        CompositeIndexingExecutionEngine engine = new CompositeIndexingExecutionEngine(plugins, indexSettings, null, null, tracking);

        CatalogSnapshotManager csm = new CatalogSnapshotManager(0, 0, 0, List.of(), 0, Map.of());
        engine.setCatalogSnapshotManager(csm);

        engine.flush();
        assertTrue("commit() must be called during flush", tracking.commitCalled);
        assertNotNull("commit() must receive commit data", tracking.lastCommitData);
    }

    public void testFlushPropagatesIOExceptionFromCommit() {
        Committer failingCommit = new Committer() {
            @Override
            public void init(CommitterSettings settings) {}

            @Override
            public void commit(Map<String, String> commitData) throws IOException {
                throw new IOException("commit failed");
            }

            @Override
            public void close() {}

            @Override
            public Map<String, String> getLastCommittedData() {
                return Map.of();
            }

            @Override
            public CommitStats getCommitStats() {
                return null;
            }

            @Override
            public SafeCommitInfo getSafeCommitInfo() {
                return SafeCommitInfo.EMPTY;
            }
        };

        Map<String, DataFormatPlugin> plugins = new HashMap<>();
        plugins.put("lucene", CompositeTestHelper.stubPlugin("lucene", 1));
        IndexSettings indexSettings = createIndexSettings("lucene");

        CompositeIndexingExecutionEngine engine = new CompositeIndexingExecutionEngine(plugins, indexSettings, null, null, failingCommit);

        CatalogSnapshotManager csm = new CatalogSnapshotManager(0, 0, 0, List.of(), 0, Map.of());
        engine.setCatalogSnapshotManager(csm);

        IOException ex = expectThrows(IOException.class, engine::flush);
        assertTrue(ex.getMessage().contains("commit failed"));
    }

    public void testFlushThrowsWhenCatalogSnapshotManagerNotSet() {
        CompositeIndexingExecutionEngine engine = CompositeTestHelper.createStubEngine("lucene");

        IllegalStateException ex = expectThrows(IllegalStateException.class, engine::flush);
        assertTrue(ex.getMessage().contains("CatalogSnapshotManager not set"));
    }

    /**
     * A Committer that tracks which methods were called, for test assertions.
     */
    private static class TrackingCommitter implements Committer {
        boolean initCalled = false;
        boolean commitCalled = false;
        boolean closeCalled = false;
        Map<String, String> lastCommitData = null;

        @Override
        public void init(CommitterSettings settings) {
            initCalled = true;
        }

        @Override
        public void commit(Map<String, String> commitData) {
            commitCalled = true;
            lastCommitData = commitData;
        }

        @Override
        public void close() {
            closeCalled = true;
        }

        @Override
        public Map<String, String> getLastCommittedData() {
            return Map.of();
        }

        @Override
        public CommitStats getCommitStats() {
            return null;
        }

        @Override
        public SafeCommitInfo getSafeCommitInfo() {
            return SafeCommitInfo.EMPTY;
        }
    }

    private IndexSettings createIndexSettings(String primaryFormat) {
        Settings settings = Settings.builder()
            .put("index.composite.primary_data_format", primaryFormat)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("test-index").settings(settings).build();
        return new IndexSettings(indexMetadata, Settings.EMPTY);
    }
}
