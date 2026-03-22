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
import org.opensearch.index.engine.dataformat.DataFormatPlugin;
import org.opensearch.index.engine.dataformat.DataformatAwareLockableWriterPool;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentLinkedQueue;

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
        assertEquals("parquet", engine.getSecondaryDelegates().get(0).getDataFormat().name());
    }

    public void testConstructorWithMultipleSecondaries() {
        CompositeIndexingExecutionEngine engine = CompositeTestHelper.createStubEngine("lucene", "parquet", "arrow");
        assertEquals(2, engine.getSecondaryDelegates().size());
    }

    public void testConstructorThrowsWhenCompositeDisabled() {
        Map<String, DataFormatPlugin> plugins = new HashMap<>();
        plugins.put("lucene", CompositeTestHelper.stubPlugin("lucene", 1));

        Settings settings = Settings.builder()
            .put("index.composite.enabled", false)
            .put("index.composite.primary_data_format", "lucene")
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("test-index").settings(settings).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);

        DataformatAwareLockableWriterPool<CompositeWriter> pool = stubPool();
        expectThrows(IllegalStateException.class, () -> new CompositeIndexingExecutionEngine(plugins, indexSettings, null, null, pool));
    }

    public void testConstructorThrowsWhenPrimaryFormatNotRegistered() {
        Map<String, DataFormatPlugin> plugins = new HashMap<>();
        plugins.put("lucene", CompositeTestHelper.stubPlugin("lucene", 1));

        IndexSettings indexSettings = createIndexSettings(true, "parquet");
        DataformatAwareLockableWriterPool<CompositeWriter> pool = stubPool();
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new CompositeIndexingExecutionEngine(plugins, indexSettings, null, null, pool)
        );
        assertTrue(ex.getMessage().contains("parquet"));
    }

    public void testConstructorThrowsWhenSecondaryFormatNotRegistered() {
        Map<String, DataFormatPlugin> plugins = new HashMap<>();
        plugins.put("lucene", CompositeTestHelper.stubPlugin("lucene", 1));

        Settings settings = Settings.builder()
            .put("index.composite.enabled", true)
            .put("index.composite.primary_data_format", "lucene")
            .putList("index.composite.secondary_data_formats", "parquet")
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("test-index").settings(settings).build();
        IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);

        DataformatAwareLockableWriterPool<CompositeWriter> pool = stubPool();
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> new CompositeIndexingExecutionEngine(plugins, indexSettings, null, null, pool)
        );
        assertTrue(ex.getMessage().contains("parquet"));
    }

    public void testConstructorRejectsNullDataFormatPlugins() {
        IndexSettings indexSettings = createIndexSettings(true, "lucene");
        DataformatAwareLockableWriterPool<CompositeWriter> pool = stubPool();
        expectThrows(NullPointerException.class, () -> new CompositeIndexingExecutionEngine(null, indexSettings, null, null, pool));
    }

    public void testConstructorRejectsNullIndexSettings() {
        Map<String, DataFormatPlugin> plugins = Map.of("lucene", CompositeTestHelper.stubPlugin("lucene", 1));
        DataformatAwareLockableWriterPool<CompositeWriter> pool = stubPool();
        expectThrows(NullPointerException.class, () -> new CompositeIndexingExecutionEngine(plugins, null, null, null, pool));
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

    private IndexSettings createIndexSettings(boolean compositeEnabled, String primaryFormat) {
        Settings settings = Settings.builder()
            .put("index.composite.enabled", compositeEnabled)
            .put("index.composite.primary_data_format", primaryFormat)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .build();
        IndexMetadata indexMetadata = IndexMetadata.builder("test-index").settings(settings).build();
        return new IndexSettings(indexMetadata, Settings.EMPTY);
    }

    /**
     * Creates an uninitialized pool for tests that only exercise constructor validation
     * and expect exceptions before the pool is ever used.
     */
    private static DataformatAwareLockableWriterPool<CompositeWriter> stubPool() {
        return new DataformatAwareLockableWriterPool<>(ConcurrentLinkedQueue::new, 1);
    }
}
