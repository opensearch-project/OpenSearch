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
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatPlugin;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.RefreshInput;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

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
        DataFormatRegistry registry = mock(DataFormatRegistry.class);
        when(registry.format("parquet")).thenReturn(null);
        when(registry.getRegisteredFormats()).thenReturn(Set.of(CompositeTestHelper.stubFormat("lucene", 1, Set.of())));

        IndexSettings indexSettings = createIndexSettings("parquet");
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new CompositeIndexingExecutionEngine(indexSettings, null, new CompositeTestHelper.StubCommitter(), registry, null, null)
        );
        assertTrue(ex.getMessage().contains("parquet"));
    }

    public void testConstructorThrowsWhenSecondaryFormatNotRegistered() {
        DataFormatRegistry registry = mock(DataFormatRegistry.class);
        when(registry.format("lucene")).thenReturn(CompositeTestHelper.stubFormat("lucene", 1, Set.of()));
        when(registry.format("parquet")).thenReturn(null);
        when(registry.getRegisteredFormats()).thenReturn(Set.of(CompositeTestHelper.stubFormat("lucene", 1, Set.of())));
        when(registry.getIndexingEngine(any(), any())).thenAnswer(invocation -> {
            DataFormatPlugin plugin = CompositeTestHelper.stubPlugin("lucene", 1);
            return plugin.indexingEngine(null);
        });

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
            () -> new CompositeIndexingExecutionEngine(indexSettings, null, new CompositeTestHelper.StubCommitter(), registry, null, null)
        );
        assertTrue(ex.getMessage().contains("parquet"));
    }

    public void testConstructorRejectsNullDataFormatRegistry() {
        IndexSettings indexSettings = createIndexSettings("lucene");
        expectThrows(
            NullPointerException.class,
            () -> new CompositeIndexingExecutionEngine(indexSettings, null, new CompositeTestHelper.StubCommitter(), null, null, null)
        );
    }

    public void testConstructorRejectsNullIndexSettings() {
        DataFormatRegistry registry = mock(DataFormatRegistry.class);
        expectThrows(
            NullPointerException.class,
            () -> new CompositeIndexingExecutionEngine(null, null, new CompositeTestHelper.StubCommitter(), registry, null, null)

        );
    }

    public void testValidateFormatsRegisteredAcceptsValidConfig() {
        DataFormatRegistry registry = mock(DataFormatRegistry.class);
        when(registry.format("lucene")).thenReturn(CompositeTestHelper.stubFormat("lucene", 1, Set.of()));
        when(registry.format("parquet")).thenReturn(CompositeTestHelper.stubFormat("parquet", 2, Set.of()));

        CompositeIndexingExecutionEngine.validateFormatsRegistered(registry, "lucene", List.of("parquet"));
    }

    public void testValidateFormatsRegisteredRejectsMissingPrimary() {
        DataFormatRegistry registry = mock(DataFormatRegistry.class);
        when(registry.format("parquet")).thenReturn(null);
        when(registry.getRegisteredFormats()).thenReturn(Set.of(CompositeTestHelper.stubFormat("lucene", 1, Set.of())));

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> CompositeIndexingExecutionEngine.validateFormatsRegistered(registry, "parquet", List.of())
        );
        assertTrue(ex.getMessage().contains("parquet"));
    }

    public void testValidateFormatsRegisteredRejectsMissingSecondary() {
        DataFormatRegistry registry = mock(DataFormatRegistry.class);
        when(registry.format("lucene")).thenReturn(CompositeTestHelper.stubFormat("lucene", 1, Set.of()));
        when(registry.format("parquet")).thenReturn(null);
        when(registry.getRegisteredFormats()).thenReturn(Set.of(CompositeTestHelper.stubFormat("lucene", 1, Set.of())));

        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> CompositeIndexingExecutionEngine.validateFormatsRegistered(registry, "lucene", List.of("parquet"))
        );
        assertTrue(ex.getMessage().contains("parquet"));
    }

    public void testValidateFormatsRegisteredRejectsSecondaryEqualToPrimary() {
        DataFormatRegistry registry = mock(DataFormatRegistry.class);
        when(registry.format("lucene")).thenReturn(CompositeTestHelper.stubFormat("lucene", 1, Set.of()));

        IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> CompositeIndexingExecutionEngine.validateFormatsRegistered(registry, "lucene", List.of("lucene"))
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
        assertNotNull(engine.getMerger());
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

    // --- Property test — Committer is required ---

    public void testConstructorThrowsWhenCommitterNull() {
        IndexSettings indexSettings = createIndexSettings("lucene");
        DataFormatRegistry registry = mock(DataFormatRegistry.class);

        IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> new CompositeIndexingExecutionEngine(indexSettings, null, null, registry, null, null)
        );
        assertTrue(ex.getMessage().contains("Committer must not be null"));
    }

    // --- Property test — Refresh never calls Committer methods ---

    public void testRefreshNeverCallsCommitterMethods() throws IOException {
        TrackingCommitter tracking = new TrackingCommitter();
        DataFormat luceneFormat = CompositeTestHelper.stubFormat("lucene", 1, Set.of());
        DataFormatRegistry registry = mock(DataFormatRegistry.class);
        when(registry.format("lucene")).thenReturn(luceneFormat);
        doReturn(new CompositeTestHelper.StubIndexingExecutionEngine(luceneFormat)).when(registry).getIndexingEngine(any(), any());
        IndexSettings indexSettings = createIndexSettings("lucene");

        CompositeIndexingExecutionEngine engine = new CompositeIndexingExecutionEngine(indexSettings, null, tracking, registry, null, null);

        // Reset tracking after construction (init is called during construction)
        tracking.commitCalled = false;

        RefreshInput refreshInput = RefreshInput.builder().build();
        engine.refresh(refreshInput);

        assertFalse("commit() must not be called during refresh", tracking.commitCalled);
    }

    /**
     * A Committer that tracks which methods were called, for test assertions.
     */
    private static class TrackingCommitter implements Committer {
        boolean commitCalled = false;
        boolean closeCalled = false;
        Map<String, String> lastCommitData = null;

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

        @Override
        public List<CatalogSnapshot> listCommittedSnapshots() {
            return List.of();
        }

        @Override
        public void deleteCommit(CatalogSnapshot snapshot) {}

        @Override
        public boolean isCommitManagedFile(String fileName) {
            return false;
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
