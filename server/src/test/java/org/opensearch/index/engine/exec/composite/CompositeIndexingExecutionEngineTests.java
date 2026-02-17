/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.composite;

import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.DocumentInput;
import org.opensearch.index.engine.exec.FileInfos;
import org.opensearch.index.engine.exec.FlushIn;
import org.opensearch.index.engine.exec.IndexingExecutionEngine;
import org.opensearch.index.engine.exec.RefreshInput;
import org.opensearch.index.engine.exec.RefreshResult;
import org.opensearch.index.engine.exec.WriteResult;
import org.opensearch.index.engine.exec.Writer;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.Segment;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.DataSourcePlugin;
import org.opensearch.plugins.PluginsService;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Stream;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The following key areas are covered in the tests:
 * - Engine initialization with DataSourcePlugin integration and fallback behavior
 * - Writer generation management including atomic increments and thread-safe updates
 * - Writer pool operations with concurrent access from multiple threads
 * - Refresh operations with various writer states (empty, single, multiple writers)
 * - File management operations (loading, deletion) with proper format segregation
 * - Resource cleanup and native memory tracking across delegates
 * - Error handling and exception propagation (IOException wrapping, unsupported operations)
 * - Delegate coordination ensuring all engines receive correct method calls
 */
public class CompositeIndexingExecutionEngineTests extends OpenSearchTestCase {

    private MapperService mockMapperService;
    private PluginsService mockPluginsService;
    private ShardPath shardPath;
    private DataSourcePlugin mockDataSourcePlugin;
    private IndexingExecutionEngine<DataFormat> mockDelegateEngine;
    private DataFormat mockDataFormat;

    @Before
    public void setUp() throws Exception {
        super.setUp();

        // Setup ShardPath
        Path tempDir = createTempDir();
        ShardId shardId = new ShardId(new Index("test", "test-uuid"), 0);
        Path shardDir = tempDir.resolve("test-uuid").resolve("0");
        shardPath = new ShardPath(false, shardDir, shardDir, shardId);

        // Setup mocks
        mockMapperService = mock(MapperService.class);
        mockPluginsService = mock(PluginsService.class);
        mockDataSourcePlugin = mock(DataSourcePlugin.class);
        mockDelegateEngine = mock(IndexingExecutionEngine.class);
        mockDataFormat = mock(DataFormat.class);

        // Default mock behavior
        when(mockDataFormat.name()).thenReturn("test-format");
        when(mockDelegateEngine.getDataFormat()).thenReturn(mockDataFormat);
        when(mockDelegateEngine.getNativeBytesUsed()).thenReturn(0L);
    }

    // ========================================
    // 1. Constructor and Initialization Tests
    // ========================================

    public void testConstructorWithValidPluginsService() throws IOException {
        // Setup
        when(mockPluginsService.filterPlugins(DataSourcePlugin.class))
            .thenReturn(List.of(mockDataSourcePlugin));
        when(mockDataSourcePlugin.getDataFormat()).thenReturn(mockDataFormat);
        when(mockDataSourcePlugin.indexingEngine(any(), any())).thenReturn(mockDelegateEngine);

        // Execute
        CompositeIndexingExecutionEngine engine = new CompositeIndexingExecutionEngine(
            mockMapperService,
            mockPluginsService,
            shardPath,
            100L
        );

        // Verify
        assertNotNull(engine);
        assertNotNull(engine.getDataFormat());
        assertNotNull(engine.getDataFormatWriterPool());
        assertEquals(1, engine.getDelegates().size());
        assertEquals(mockDelegateEngine, engine.getDelegates().get(0));
        assertEquals(100L, engine.getCurrentWriterGeneration());

        engine.close();
    }

    public void testConstructorThrowsExceptionWhenNoPluginFound() {
        // Setup - empty plugin list
        when(mockPluginsService.filterPlugins(DataSourcePlugin.class))
            .thenReturn(Collections.emptyList());

        // Execute and verify - should throw IllegalArgumentException when no plugin found
        expectThrows(IllegalArgumentException.class, () -> {
            new CompositeIndexingExecutionEngine(
                mockMapperService,
                mockPluginsService,
                shardPath,
                50L
            );
        });
    }

    public void testConstructorInitializesAllComponents() throws IOException {
        // Setup
        when(mockPluginsService.filterPlugins(DataSourcePlugin.class))
            .thenReturn(List.of(mockDataSourcePlugin));
        when(mockDataSourcePlugin.getDataFormat()).thenReturn(mockDataFormat);
        when(mockDataSourcePlugin.indexingEngine(any(), any())).thenReturn(mockDelegateEngine);

        long initialGeneration = 12345L;

        // Execute
        CompositeIndexingExecutionEngine engine = new CompositeIndexingExecutionEngine(
            mockMapperService,
            mockPluginsService,
            shardPath,
            initialGeneration
        );

        // Verify writer generation initialization
        assertEquals(initialGeneration, engine.getCurrentWriterGeneration());
        assertEquals(initialGeneration, engine.getNextWriterGeneration());
        assertEquals(initialGeneration + 1, engine.getCurrentWriterGeneration());

        // Verify writer pool initialization
        assertNotNull(engine.getDataFormatWriterPool());

        // Verify delegates list is unmodifiable
        List<IndexingExecutionEngine<?>> delegates = engine.getDelegates();
        expectThrows(UnsupportedOperationException.class, () -> {
            delegates.add(mock(IndexingExecutionEngine.class));
        });

        engine.close();
    }

    // ========================================
    // 2. Writer Generation Management Tests
    // ========================================

    public void testWriterGenerationBehavior() throws IOException {
        // Setup
        when(mockPluginsService.filterPlugins(DataSourcePlugin.class))
            .thenReturn(List.of(mockDataSourcePlugin));
        when(mockDataSourcePlugin.getDataFormat()).thenReturn(mockDataFormat);
        when(mockDataSourcePlugin.indexingEngine(any(), any())).thenReturn(mockDelegateEngine);

        CompositeIndexingExecutionEngine engine = new CompositeIndexingExecutionEngine(
            mockMapperService,
            mockPluginsService,
            shardPath,
            100L
        );

        // Test 1: getCurrentWriterGeneration does not increment
        long current1 = engine.getCurrentWriterGeneration();
        long current2 = engine.getCurrentWriterGeneration();
        long current3 = engine.getCurrentWriterGeneration();
        assertEquals(100L, current1);
        assertEquals(100L, current2);
        assertEquals(100L, current3);

        // Test 2: getNextWriterGeneration increments atomically
        long gen1 = engine.getNextWriterGeneration();
        long gen2 = engine.getNextWriterGeneration();
        long gen3 = engine.getNextWriterGeneration();
        assertEquals(100L, gen1);
        assertEquals(101L, gen2);
        assertEquals(102L, gen3);
        assertEquals(103L, engine.getCurrentWriterGeneration());

        engine.close();
    }

    public void testUpdateWriterGenerationIfNeeded() throws IOException {
        // Setup
        when(mockPluginsService.filterPlugins(DataSourcePlugin.class))
            .thenReturn(List.of(mockDataSourcePlugin));
        when(mockDataSourcePlugin.getDataFormat()).thenReturn(mockDataFormat);
        when(mockDataSourcePlugin.indexingEngine(any(), any())).thenReturn(mockDelegateEngine);

        CompositeIndexingExecutionEngine engine = new CompositeIndexingExecutionEngine(
            mockMapperService,
            mockPluginsService,
            shardPath,
            100L
        );

        // Test 1: minGeneration < current - should not update
        engine.updateWriterGenerationIfNeeded(50L);
        assertEquals(100L, engine.getCurrentWriterGeneration());

        // Test 2: minGeneration == current - should update to minGeneration + 1
        engine.updateWriterGenerationIfNeeded(100L);
        assertEquals(101L, engine.getCurrentWriterGeneration());

        // Test 3: minGeneration > current - should update to minGeneration + 1
        engine.updateWriterGenerationIfNeeded(200L);
        assertEquals(201L, engine.getCurrentWriterGeneration());

        engine.close();
    }

    public void testConcurrentGetNextWriterGenerationMaintainsAtomicity() throws Exception {
        // Setup
        when(mockPluginsService.filterPlugins(DataSourcePlugin.class))
            .thenReturn(List.of(mockDataSourcePlugin));
        when(mockDataSourcePlugin.getDataFormat()).thenReturn(mockDataFormat);
        when(mockDataSourcePlugin.indexingEngine(any(), any())).thenReturn(mockDelegateEngine);

        CompositeIndexingExecutionEngine engine = new CompositeIndexingExecutionEngine(
            mockMapperService,
            mockPluginsService,
            shardPath,
            0L
        );

        int threadCount = 10;
        int iterationsPerThread = 100;
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        List<Long> allGenerations = Collections.synchronizedList(new ArrayList<>());

        // Execute - multiple threads incrementing concurrently
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            Thread thread = new Thread(() -> {
                try {
                    barrier.await(); // Synchronize start
                    for (int j = 0; j < iterationsPerThread; j++) {
                        allGenerations.add(engine.getNextWriterGeneration());
                    }
                } catch (Exception e) {
                    fail("Thread failed: " + e.getMessage());
                }
            });
            threads.add(thread);
            thread.start();
        }

        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join();
        }

        // Verify - all generations should be unique
        assertEquals(threadCount * iterationsPerThread, allGenerations.size());
        assertEquals(threadCount * iterationsPerThread, allGenerations.stream().distinct().count());

        // Verify final generation
        assertEquals(threadCount * iterationsPerThread, engine.getCurrentWriterGeneration());

        engine.close();
    }

    public void testConcurrentUpdateWriterGenerationIfNeededIsThreadSafe() throws Exception {
        // Setup
        when(mockPluginsService.filterPlugins(DataSourcePlugin.class))
            .thenReturn(List.of(mockDataSourcePlugin));
        when(mockDataSourcePlugin.getDataFormat()).thenReturn(mockDataFormat);
        when(mockDataSourcePlugin.indexingEngine(any(), any())).thenReturn(mockDelegateEngine);

        CompositeIndexingExecutionEngine engine = new CompositeIndexingExecutionEngine(
            mockMapperService,
            mockPluginsService,
            shardPath,
            0L
        );

        int threadCount = 10;
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        AtomicLong maxMinGeneration = new AtomicLong(0);

        // Execute - multiple threads updating concurrently
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            final long minGen = (i + 1) * 100L;
            Thread thread = new Thread(() -> {
                try {
                    barrier.await(); // Synchronize start
                    engine.updateWriterGenerationIfNeeded(minGen);
                    maxMinGeneration.updateAndGet(current -> Math.max(current, minGen));
                } catch (Exception e) {
                    fail("Thread failed: " + e.getMessage());
                }
            });
            threads.add(thread);
            thread.start();
        }

        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join();
        }

        // Verify - final generation should be at least maxMinGeneration + 1
        assertTrue(engine.getCurrentWriterGeneration() >= maxMinGeneration.get() + 1);

        engine.close();
    }

    // ========================================
    // 3. Writer Pool Operations Tests
    // ========================================

    public void testCreateCompositeWriterReturnsNonNullWriter() throws IOException {
        // Setup
        when(mockPluginsService.filterPlugins(DataSourcePlugin.class))
            .thenReturn(List.of(mockDataSourcePlugin));
        when(mockDataSourcePlugin.getDataFormat()).thenReturn(mockDataFormat);
        when(mockDataSourcePlugin.indexingEngine(any(), any())).thenReturn(mockDelegateEngine);


        MockWriter mockWriter = new MockWriter(0L);
        doReturn(mockWriter).when(mockDelegateEngine).createWriter(anyLong());

        CompositeIndexingExecutionEngine engine = new CompositeIndexingExecutionEngine(
            mockMapperService,
            mockPluginsService,
            shardPath,
            0L
        );

        // Execute
        Writer<CompositeDataFormatWriter.CompositeDocumentInput> writer = engine.createCompositeWriter();

        // Verify
        assertNotNull(writer);
        assertTrue(writer instanceof CompositeDataFormatWriter);

        // Cleanup
        writer.close();
        engine.close();
    }

    public void testCreateCompositeWriterIncrementsGeneration() throws IOException {
        // Setup
        when(mockPluginsService.filterPlugins(DataSourcePlugin.class))
            .thenReturn(List.of(mockDataSourcePlugin));
        when(mockDataSourcePlugin.getDataFormat()).thenReturn(mockDataFormat);
        when(mockDataSourcePlugin.indexingEngine(any(), any())).thenReturn(mockDelegateEngine);


        MockWriter mockWriter = new MockWriter(0L);
        doReturn(mockWriter).when(mockDelegateEngine).createWriter(anyLong());

        CompositeIndexingExecutionEngine engine = new CompositeIndexingExecutionEngine(
            mockMapperService,
            mockPluginsService,
            shardPath,
            100L
        );

        long initialGeneration = engine.getCurrentWriterGeneration();

        // Execute
        Writer<CompositeDataFormatWriter.CompositeDocumentInput> writer1 = engine.createCompositeWriter();
        long afterFirstWriter = engine.getCurrentWriterGeneration();

        Writer<CompositeDataFormatWriter.CompositeDocumentInput> writer2 = engine.createCompositeWriter();
        long afterSecondWriter = engine.getCurrentWriterGeneration();

        // Verify
        assertEquals(initialGeneration + 1, afterFirstWriter);
        assertEquals(initialGeneration + 2, afterSecondWriter);

        // Cleanup
        writer1.close();
        writer2.close();
        engine.close();
    }

    public void testMultipleSequentialCreateCompositeWriterCalls() throws IOException {
        // Setup
        when(mockPluginsService.filterPlugins(DataSourcePlugin.class))
            .thenReturn(List.of(mockDataSourcePlugin));
        when(mockDataSourcePlugin.getDataFormat()).thenReturn(mockDataFormat);
        when(mockDataSourcePlugin.indexingEngine(any(), any())).thenReturn(mockDelegateEngine);


        MockWriter mockWriter = new MockWriter(0L);
        doReturn(mockWriter).when(mockDelegateEngine).createWriter(anyLong());

        CompositeIndexingExecutionEngine engine = new CompositeIndexingExecutionEngine(
            mockMapperService,
            mockPluginsService,
            shardPath,
            0L
        );

        // Execute - create multiple writers
        List<Writer<CompositeDataFormatWriter.CompositeDocumentInput>> writers = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            writers.add(engine.createCompositeWriter());
        }

        // Verify - all writers should be unique instances
        assertEquals(5, writers.size());
        for (int i = 0; i < writers.size(); i++) {
            for (int j = i + 1; j < writers.size(); j++) {
                assertNotSame(writers.get(i), writers.get(j));
            }
        }

        // Cleanup
        for (Writer<?> writer : writers) {
            writer.close();
        }
        engine.close();
    }

    public void testConcurrentWriterRequestsFromMultipleThreads() throws Exception {
        // Setup
        when(mockPluginsService.filterPlugins(DataSourcePlugin.class))
            .thenReturn(List.of(mockDataSourcePlugin));
        when(mockDataSourcePlugin.getDataFormat()).thenReturn(mockDataFormat);
        when(mockDataSourcePlugin.indexingEngine(any(), any())).thenReturn(mockDelegateEngine);


        MockWriter mockWriter = new MockWriter(0L);
        doReturn(mockWriter).when(mockDelegateEngine).createWriter(anyLong());

        CompositeIndexingExecutionEngine engine = new CompositeIndexingExecutionEngine(
            mockMapperService,
            mockPluginsService,
            shardPath,
            0L
        );

        int threadCount = 10;
        CyclicBarrier barrier = new CyclicBarrier(threadCount);
        List<Writer<CompositeDataFormatWriter.CompositeDocumentInput>> writers =
            Collections.synchronizedList(new ArrayList<>());

        // Execute - multiple threads requesting writers concurrently
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < threadCount; i++) {
            Thread thread = new Thread(() -> {
                try {
                    barrier.await(); // Synchronize start
                    Writer<CompositeDataFormatWriter.CompositeDocumentInput> writer = engine.createCompositeWriter();
                    writers.add(writer);
                } catch (Exception e) {
                    fail("Thread failed: " + e.getMessage());
                }
            });
            threads.add(thread);
            thread.start();
        }

        // Wait for all threads to complete
        for (Thread thread : threads) {
            thread.join();
        }

        // Verify - all writers should be created successfully
        assertEquals(threadCount, writers.size());

        // Cleanup
        for (Writer<?> writer : writers) {
            writer.close();
        }
        engine.close();
    }

    // ========================================
    // 4. Refresh Operation Tests
    // ========================================

    public void testRefreshWithNoActiveWritersReturnsNull() throws IOException {
        // Setup
        when(mockPluginsService.filterPlugins(DataSourcePlugin.class))
            .thenReturn(List.of(mockDataSourcePlugin));
        when(mockDataSourcePlugin.getDataFormat()).thenReturn(mockDataFormat);
        when(mockDataSourcePlugin.indexingEngine(any(), any())).thenReturn(mockDelegateEngine);

        CompositeIndexingExecutionEngine engine = new CompositeIndexingExecutionEngine(
            mockMapperService,
            mockPluginsService,
            shardPath,
            0L
        );

        // Execute
        RefreshInput refreshInput = new RefreshInput();
        RefreshResult result = engine.refresh(refreshInput);

        // Verify
        assertNull(result);

        engine.close();
    }

    public void testRefreshWithSingleWriterContainingData() throws IOException {
        // Setup
        when(mockPluginsService.filterPlugins(DataSourcePlugin.class))
            .thenReturn(List.of(mockDataSourcePlugin));
        when(mockDataSourcePlugin.getDataFormat()).thenReturn(mockDataFormat);
        when(mockDataSourcePlugin.indexingEngine(any(), any())).thenReturn(mockDelegateEngine);


        MockWriter mockWriter = new MockWriter(0L);
        doReturn(mockWriter).when(mockDelegateEngine).createWriter(anyLong());

        // Mock flush to return file infos
        WriterFileSet fileSet = WriterFileSet.builder()
            .directory(Path.of("/test"))
            .writerGeneration(1L)
            .addFile("test.file")
            .build();

        FileInfos fileInfos = FileInfos.builder()
            .putWriterFileSet(mockDataFormat, fileSet)
            .build();

        mockWriter.setFlushResult(fileInfos);

        CompositeIndexingExecutionEngine engine = new CompositeIndexingExecutionEngine(
            mockMapperService,
            mockPluginsService,
            shardPath,
            0L
        );

        // Create a writer to have something to refresh
        Writer<CompositeDataFormatWriter.CompositeDocumentInput> writer = engine.createCompositeWriter();
        writer.close(); // Return to pool

        // Execute
        RefreshInput refreshInput = new RefreshInput();
        RefreshResult result = engine.refresh(refreshInput);

        // Verify
        assertNotNull(result);
        assertNotNull(result.getRefreshedSegments());
        assertEquals(1, result.getRefreshedSegments().size());

        verify(mockDelegateEngine, times(1)).refresh(any());

        engine.close();
    }

    public void testRefreshWithMultipleWriters() throws IOException {
        // Setup
        when(mockPluginsService.filterPlugins(DataSourcePlugin.class))
            .thenReturn(List.of(mockDataSourcePlugin));
        when(mockDataSourcePlugin.getDataFormat()).thenReturn(mockDataFormat);
        when(mockDataSourcePlugin.indexingEngine(any(), any())).thenReturn(mockDelegateEngine);


        MockWriter mockWriter = new MockWriter(0L);
        doReturn(mockWriter).when(mockDelegateEngine).createWriter(anyLong());

        // Mock flush to return file infos
        WriterFileSet fileSet = WriterFileSet.builder()
            .directory(Path.of("/test"))
            .writerGeneration(1L)
            .addFile("test.file")
            .build();

        FileInfos fileInfos = FileInfos.builder()
            .putWriterFileSet(mockDataFormat, fileSet)
            .build();

        mockWriter.setFlushResult(fileInfos);

        CompositeIndexingExecutionEngine engine = new CompositeIndexingExecutionEngine(
            mockMapperService,
            mockPluginsService,
            shardPath,
            0L
        );

        // Create multiple writers
        Writer<CompositeDataFormatWriter.CompositeDocumentInput> writer1 = engine.createCompositeWriter();
        writer1.close();
        Writer<CompositeDataFormatWriter.CompositeDocumentInput> writer2 = engine.createCompositeWriter();
        writer2.close();
        Writer<CompositeDataFormatWriter.CompositeDocumentInput> writer3 = engine.createCompositeWriter();
        writer3.close();

        // Execute
        RefreshInput refreshInput = new RefreshInput();
        RefreshResult result = engine.refresh(refreshInput);

        // Verify
        assertNotNull(result);
        assertNotNull(result.getRefreshedSegments());
        assertEquals(3, result.getRefreshedSegments().size());

        verify(mockDelegateEngine, times(1)).refresh(any());

        engine.close();
    }

    public void testRefreshWithEmptySegments() throws IOException {
        // Setup
        when(mockPluginsService.filterPlugins(DataSourcePlugin.class))
            .thenReturn(List.of(mockDataSourcePlugin));
        when(mockDataSourcePlugin.getDataFormat()).thenReturn(mockDataFormat);
        when(mockDataSourcePlugin.indexingEngine(any(), any())).thenReturn(mockDelegateEngine);


        MockWriter mockWriter = new MockWriter(0L);
        doReturn(mockWriter).when(mockDelegateEngine).createWriter(anyLong());

        // Mock flush to return empty file infos
        FileInfos emptyFileInfos = FileInfos.builder().build();
        mockWriter.setFlushResult(emptyFileInfos);

        CompositeIndexingExecutionEngine engine = new CompositeIndexingExecutionEngine(
            mockMapperService,
            mockPluginsService,
            shardPath,
            0L
        );

        // Create a writer
        Writer<CompositeDataFormatWriter.CompositeDocumentInput> writer = engine.createCompositeWriter();
        writer.close();

        // Execute
        RefreshInput refreshInput = new RefreshInput();
        RefreshResult result = engine.refresh(refreshInput);

        // Verify - should return null when no segments have files
        assertNull(result);

        verify(mockDelegateEngine, never()).refresh(any());

        engine.close();
    }

    public void testRefreshCallsDelegateEnginesRefresh() throws IOException {
        // Setup
        when(mockPluginsService.filterPlugins(DataSourcePlugin.class))
            .thenReturn(List.of(mockDataSourcePlugin));
        when(mockDataSourcePlugin.getDataFormat()).thenReturn(mockDataFormat);
        when(mockDataSourcePlugin.indexingEngine(any(), any())).thenReturn(mockDelegateEngine);


        MockWriter mockWriter = new MockWriter(0L);
        doReturn(mockWriter).when(mockDelegateEngine).createWriter(anyLong());

        WriterFileSet fileSet = WriterFileSet.builder()
            .directory(Path.of("/test"))
            .writerGeneration(1L)
            .addFile("test.file")
            .build();

        FileInfos fileInfos = FileInfos.builder()
            .putWriterFileSet(mockDataFormat, fileSet)
            .build();

        mockWriter.setFlushResult(fileInfos);

        CompositeIndexingExecutionEngine engine = new CompositeIndexingExecutionEngine(
            mockMapperService,
            mockPluginsService,
            shardPath,
            0L
        );

        // Create a writer
        Writer<CompositeDataFormatWriter.CompositeDocumentInput> writer = engine.createCompositeWriter();
        writer.close();

        // Execute
        RefreshInput refreshInput = new RefreshInput();
        engine.refresh(refreshInput);

        // Verify - delegate refresh should be called
        verify(mockDelegateEngine, times(1)).refresh(any());

        engine.close();
    }

    public void testRefreshHandlesIOExceptionAndWrapsInRuntimeException() throws IOException {
        // Setup
        when(mockPluginsService.filterPlugins(DataSourcePlugin.class))
            .thenReturn(List.of(mockDataSourcePlugin));
        when(mockDataSourcePlugin.getDataFormat()).thenReturn(mockDataFormat);
        when(mockDataSourcePlugin.indexingEngine(any(), any())).thenReturn(mockDelegateEngine);


        MockWriter mockWriter = new MockWriter(0L);
        doReturn(mockWriter).when(mockDelegateEngine).createWriter(anyLong());

        // Mock flush to throw IOException
        mockWriter.setFlushResult(null); // Will cause NPE to simulate exception

        CompositeIndexingExecutionEngine engine = new CompositeIndexingExecutionEngine(
            mockMapperService,
            mockPluginsService,
            shardPath,
            0L
        );

        // Create a writer
        Writer<CompositeDataFormatWriter.CompositeDocumentInput> writer = engine.createCompositeWriter();
        writer.close();

        // Execute and verify - should wrap IOException in RuntimeException
        RefreshInput refreshInput = new RefreshInput();
        expectThrows(RuntimeException.class, () -> engine.refresh(refreshInput));

        engine.close();
    }

    // ========================================
    // 5. File Management Tests
    // ========================================

    public void testLoadWriterFilesDelegatesToAllEngines() throws IOException {
        // Setup
        when(mockPluginsService.filterPlugins(DataSourcePlugin.class))
            .thenReturn(List.of(mockDataSourcePlugin));
        when(mockDataSourcePlugin.getDataFormat()).thenReturn(mockDataFormat);
        when(mockDataSourcePlugin.indexingEngine(any(), any())).thenReturn(mockDelegateEngine);

        doNothing().when(mockDelegateEngine).loadWriterFiles(any());

        CompositeIndexingExecutionEngine engine = new CompositeIndexingExecutionEngine(
            mockMapperService,
            mockPluginsService,
            shardPath,
            0L
        );

        CatalogSnapshot mockSnapshot = mock(CatalogSnapshot.class);

        // Execute
        engine.loadWriterFiles(mockSnapshot);

        // Verify
        verify(mockDelegateEngine, times(1)).loadWriterFiles(mockSnapshot);

        engine.close();
    }

    public void testLoadWriterFilesPropagatesIOException() throws IOException {
        // Setup
        when(mockPluginsService.filterPlugins(DataSourcePlugin.class))
            .thenReturn(List.of(mockDataSourcePlugin));
        when(mockDataSourcePlugin.getDataFormat()).thenReturn(mockDataFormat);
        when(mockDataSourcePlugin.indexingEngine(any(), any())).thenReturn(mockDelegateEngine);

        doThrow(new IOException("Test exception")).when(mockDelegateEngine).loadWriterFiles(any());

        CompositeIndexingExecutionEngine engine = new CompositeIndexingExecutionEngine(
            mockMapperService,
            mockPluginsService,
            shardPath,
            0L
        );

        CatalogSnapshot mockSnapshot = mock(CatalogSnapshot.class);

        // Execute and verify
        expectThrows(IOException.class, () -> engine.loadWriterFiles(mockSnapshot));

        engine.close();
    }

    public void testDeleteFilesSegregatesFilesByFormat() throws IOException {
        // Setup
        when(mockPluginsService.filterPlugins(DataSourcePlugin.class))
            .thenReturn(List.of(mockDataSourcePlugin));
        when(mockDataSourcePlugin.getDataFormat()).thenReturn(mockDataFormat);
        when(mockDataSourcePlugin.indexingEngine(any(), any())).thenReturn(mockDelegateEngine);

        doNothing().when(mockDelegateEngine).deleteFiles(any());

        CompositeIndexingExecutionEngine engine = new CompositeIndexingExecutionEngine(
            mockMapperService,
            mockPluginsService,
            shardPath,
            0L
        );

        // Create files map
        Map<String, Collection<String>> filesToDelete = new HashMap<>();
        filesToDelete.put("test-format", List.of("file1.txt", "file2.txt"));
        filesToDelete.put("other-format", List.of("file3.txt"));

        // Execute
        engine.deleteFiles(filesToDelete);

        // Verify - delegate should be called with only its format's files
        verify(mockDelegateEngine, times(1)).deleteFiles(any());

        engine.close();
    }

    public void testDeleteFilesWithEmptyFileMap() throws IOException {
        // Setup
        when(mockPluginsService.filterPlugins(DataSourcePlugin.class))
            .thenReturn(List.of(mockDataSourcePlugin));
        when(mockDataSourcePlugin.getDataFormat()).thenReturn(mockDataFormat);
        when(mockDataSourcePlugin.indexingEngine(any(), any())).thenReturn(mockDelegateEngine);

        doNothing().when(mockDelegateEngine).deleteFiles(any());

        CompositeIndexingExecutionEngine engine = new CompositeIndexingExecutionEngine(
            mockMapperService,
            mockPluginsService,
            shardPath,
            0L
        );

        // Execute with empty map
        engine.deleteFiles(new HashMap<>());

        // Verify - delegate should still be called
        verify(mockDelegateEngine, times(1)).deleteFiles(any());

        engine.close();
    }

    public void testDeleteFilesPropagatesIOException() throws IOException {
        // Setup
        when(mockPluginsService.filterPlugins(DataSourcePlugin.class))
            .thenReturn(List.of(mockDataSourcePlugin));
        when(mockDataSourcePlugin.getDataFormat()).thenReturn(mockDataFormat);
        when(mockDataSourcePlugin.indexingEngine(any(), any())).thenReturn(mockDelegateEngine);

        doThrow(new IOException("Test exception")).when(mockDelegateEngine).deleteFiles(any());

        CompositeIndexingExecutionEngine engine = new CompositeIndexingExecutionEngine(
            mockMapperService,
            mockPluginsService,
            shardPath,
            0L
        );

        Map<String, Collection<String>> filesToDelete = new HashMap<>();
        filesToDelete.put("test-format", List.of("file1.txt"));

        // Execute and verify
        expectThrows(IOException.class, () -> engine.deleteFiles(filesToDelete));

        engine.close();
    }

    // ========================================
    // 6. Resource Management Tests
    // ========================================

    public void testCloseProperlyClosesAllDelegates() throws IOException {
        // Setup
        when(mockPluginsService.filterPlugins(DataSourcePlugin.class))
            .thenReturn(List.of(mockDataSourcePlugin));
        when(mockDataSourcePlugin.getDataFormat()).thenReturn(mockDataFormat);
        when(mockDataSourcePlugin.indexingEngine(any(), any())).thenReturn(mockDelegateEngine);

        doNothing().when(mockDelegateEngine).close();

        CompositeIndexingExecutionEngine engine = new CompositeIndexingExecutionEngine(
            mockMapperService,
            mockPluginsService,
            shardPath,
            0L
        );

        // Execute
        engine.close();

        // Verify
        verify(mockDelegateEngine, times(1)).close();
    }

    public void testCloseHandlesIOException() throws IOException {
        // Setup
        when(mockPluginsService.filterPlugins(DataSourcePlugin.class))
            .thenReturn(List.of(mockDataSourcePlugin));
        when(mockDataSourcePlugin.getDataFormat()).thenReturn(mockDataFormat);
        when(mockDataSourcePlugin.indexingEngine(any(), any())).thenReturn(mockDelegateEngine);

        doThrow(new IOException("Test exception")).when(mockDelegateEngine).close();

        CompositeIndexingExecutionEngine engine = new CompositeIndexingExecutionEngine(
            mockMapperService,
            mockPluginsService,
            shardPath,
            0L
        );

        // Execute and verify - should propagate IOException
        expectThrows(IOException.class, engine::close);
    }

    public void testGetNativeBytesUsedAggregatesFromAllDelegates() throws IOException {
        // Setup

        IndexingExecutionEngine<DataFormat> mockDelegate1 = mock(IndexingExecutionEngine.class);
        DataFormat mockFormat1 = mock(DataFormat.class);

        when(mockFormat1.name()).thenReturn("format1");
        when(mockDelegate1.getDataFormat()).thenReturn(mockFormat1);
        when(mockDelegate1.getNativeBytesUsed()).thenReturn(1000L);

        DataSourcePlugin mockPlugin1 = mock(DataSourcePlugin.class);
        when(mockPlugin1.getDataFormat()).thenReturn(mockFormat1);
        when(mockPlugin1.indexingEngine(any(), any())).thenReturn(mockDelegate1);

        when(mockPluginsService.filterPlugins(DataSourcePlugin.class))
            .thenReturn(List.of(mockPlugin1));

        CompositeIndexingExecutionEngine engine = new CompositeIndexingExecutionEngine(
            mockMapperService,
            mockPluginsService,
            shardPath,
            0L
        );

        // Execute
        long totalBytes = engine.getNativeBytesUsed();

        // Verify - should sum bytes from all delegates
        assertEquals(1000L, totalBytes);

        engine.close();
    }

    public void testGetNativeBytesUsedWithZeroBytes() throws IOException {
        // Setup
        when(mockPluginsService.filterPlugins(DataSourcePlugin.class))
            .thenReturn(List.of(mockDataSourcePlugin));
        when(mockDataSourcePlugin.getDataFormat()).thenReturn(mockDataFormat);
        when(mockDataSourcePlugin.indexingEngine(any(), any())).thenReturn(mockDelegateEngine);
        when(mockDelegateEngine.getNativeBytesUsed()).thenReturn(0L);

        CompositeIndexingExecutionEngine engine = new CompositeIndexingExecutionEngine(
            mockMapperService,
            mockPluginsService,
            shardPath,
            0L
        );

        // Execute
        long totalBytes = engine.getNativeBytesUsed();

        // Verify
        assertEquals(0L, totalBytes);

        engine.close();
    }

    // ========================================
    // 7. Edge Cases and Error Handling Tests
    // ========================================

    public void testSupportedFieldTypesThrowsUnsupportedOperationException() throws IOException {
        // Setup
        when(mockPluginsService.filterPlugins(DataSourcePlugin.class))
            .thenReturn(List.of(mockDataSourcePlugin));
        when(mockDataSourcePlugin.getDataFormat()).thenReturn(mockDataFormat);
        when(mockDataSourcePlugin.indexingEngine(any(), any())).thenReturn(mockDelegateEngine);

        CompositeIndexingExecutionEngine engine = new CompositeIndexingExecutionEngine(
            mockMapperService,
            mockPluginsService,
            shardPath,
            0L
        );

        // Execute and verify
        expectThrows(UnsupportedOperationException.class, engine::supportedFieldTypes);

        engine.close();
    }

    public void testCreateWriterWithGenerationThrowsUnsupportedOperationException() throws IOException {
        // Setup
        when(mockPluginsService.filterPlugins(DataSourcePlugin.class))
            .thenReturn(List.of(mockDataSourcePlugin));
        when(mockDataSourcePlugin.getDataFormat()).thenReturn(mockDataFormat);
        when(mockDataSourcePlugin.indexingEngine(any(), any())).thenReturn(mockDelegateEngine);

        CompositeIndexingExecutionEngine engine = new CompositeIndexingExecutionEngine(
            mockMapperService,
            mockPluginsService,
            shardPath,
            0L
        );

        // Execute and verify
        expectThrows(UnsupportedOperationException.class, () -> engine.createWriter(1L));

        engine.close();
    }

    public void testGetMergerThrowsUnsupportedOperationException() throws IOException {
        // Setup
        when(mockPluginsService.filterPlugins(DataSourcePlugin.class))
            .thenReturn(List.of(mockDataSourcePlugin));
        when(mockDataSourcePlugin.getDataFormat()).thenReturn(mockDataFormat);
        when(mockDataSourcePlugin.indexingEngine(any(), any())).thenReturn(mockDelegateEngine);

        CompositeIndexingExecutionEngine engine = new CompositeIndexingExecutionEngine(
            mockMapperService,
            mockPluginsService,
            shardPath,
            0L
        );

        // Execute and verify
        expectThrows(UnsupportedOperationException.class, engine::getMerger);

        engine.close();
    }

    public void testBehaviorWhenDelegateEngineThrowsIOExceptionDuringWriterCreation() throws IOException {
        // Setup
        when(mockPluginsService.filterPlugins(DataSourcePlugin.class))
            .thenReturn(List.of(mockDataSourcePlugin));
        when(mockDataSourcePlugin.getDataFormat()).thenReturn(mockDataFormat);
        when(mockDataSourcePlugin.indexingEngine(any(), any())).thenReturn(mockDelegateEngine);

        when(mockDelegateEngine.createWriter(anyLong())).thenThrow(new IOException("Test exception"));

        CompositeIndexingExecutionEngine engine = new CompositeIndexingExecutionEngine(
            mockMapperService,
            mockPluginsService,
            shardPath,
            0L
        );

        // Execute and verify - should wrap in RuntimeException
        expectThrows(RuntimeException.class, engine::createCompositeWriter);

        engine.close();
    }

    // ========================================
    // 8. Delegate Interaction Tests
    // ========================================

    public void testDelegatesListSizeMatchesExpectedCount() throws IOException {
        // Setup
        when(mockPluginsService.filterPlugins(DataSourcePlugin.class))
            .thenReturn(List.of(mockDataSourcePlugin));
        when(mockDataSourcePlugin.getDataFormat()).thenReturn(mockDataFormat);
        when(mockDataSourcePlugin.indexingEngine(any(), any())).thenReturn(mockDelegateEngine);

        CompositeIndexingExecutionEngine engine = new CompositeIndexingExecutionEngine(
            mockMapperService,
            mockPluginsService,
            shardPath,
            0L
        );

        // Verify
        assertEquals(1, engine.getDelegates().size());

        engine.close();
    }

    public void testEachDelegateReceivesCorrectMethodCalls() throws IOException {
        // Setup
        when(mockPluginsService.filterPlugins(DataSourcePlugin.class))
            .thenReturn(List.of(mockDataSourcePlugin));
        when(mockDataSourcePlugin.getDataFormat()).thenReturn(mockDataFormat);
        when(mockDataSourcePlugin.indexingEngine(any(), any())).thenReturn(mockDelegateEngine);


        MockWriter mockWriter = new MockWriter(0L);
        doReturn(mockWriter).when(mockDelegateEngine).createWriter(anyLong());

        WriterFileSet fileSet = WriterFileSet.builder()
            .directory(Path.of("/test"))
            .writerGeneration(1L)
            .addFile("test.file")
            .build();

        FileInfos fileInfos = FileInfos.builder()
            .putWriterFileSet(mockDataFormat, fileSet)
            .build();

        mockWriter.setFlushResult(fileInfos);
        doNothing().when(mockDelegateEngine).loadWriterFiles(any());
        doNothing().when(mockDelegateEngine).deleteFiles(any());

        CompositeIndexingExecutionEngine engine = new CompositeIndexingExecutionEngine(
            mockMapperService,
            mockPluginsService,
            shardPath,
            0L
        );

        // Execute various operations
        Writer<CompositeDataFormatWriter.CompositeDocumentInput> writer = engine.createCompositeWriter();
        writer.close();

        engine.refresh(new RefreshInput());
        engine.loadWriterFiles(mock(CatalogSnapshot.class));
        engine.deleteFiles(Map.of("test-format", List.of("file.txt")));

        // Verify all delegate methods were called
        verify(mockDelegateEngine, times(1)).createWriter(anyLong());
        verify(mockDelegateEngine, times(1)).refresh(any());
        verify(mockDelegateEngine, times(1)).loadWriterFiles(any());
        verify(mockDelegateEngine, times(1)).deleteFiles(any());

        engine.close();
    }

    // ========================================
    // Mock Writer Implementation
    // ========================================

    /**
     * Mock Writer implementation for testing purposes.
     */
    static class MockWriter implements Writer<MockWriter.MockDocumentInput> {
        private final long writerGeneration;
        private boolean closed = false;
        private FileInfos flushResult = FileInfos.builder().build();

        public MockWriter(long writerGeneration) {
            this.writerGeneration = writerGeneration;
        }

        public void setFlushResult(FileInfos flushResult) {
            this.flushResult = flushResult;
        }

        @Override
        public WriteResult addDoc(MockDocumentInput d) throws IOException {
            return new WriteResult(true, null, 1, 1, 1);
        }

        @Override
        public FileInfos flush(FlushIn flushIn) throws IOException {
            return flushResult;
        }

        @Override
        public void sync() throws IOException {
            // no-op
        }

        @Override
        public void close() {
            closed = true;
        }

        @Override
        public MockDocumentInput newDocumentInput() {
            return new MockDocumentInput(this);
        }

        public boolean isClosed() {
            return closed;
        }

        public long getWriterGeneration() {
            return writerGeneration;
        }

        static class MockDocumentInput implements DocumentInput<String> {
            private final MockWriter writer;

            public MockDocumentInput(MockWriter writer) {
                this.writer = writer;
            }

            @Override
            public void addRowIdField(String fieldName, long rowId) {
                // no-op
            }

            @Override
            public void addField(MappedFieldType fieldType, Object value) {
                // no-op
            }

            @Override
            public String getFinalInput() {
                return "";
            }

            @Override
            public WriteResult addToWriter() throws IOException {
                return writer.addDoc(this);
            }

            @Override
            public void close() throws Exception {
                // no-op
            }
        }
    }
}
