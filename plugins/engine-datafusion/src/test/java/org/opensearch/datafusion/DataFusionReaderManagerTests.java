/*
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.datafusion;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.FieldVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Field;
import org.junit.Before;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.datafusion.search.*;
import org.opensearch.env.Environment;
import org.opensearch.index.engine.exec.*;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.search.aggregations.SearchResultsCollector;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.vectorized.execution.search.DataFormat;

import static org.opensearch.index.engine.Engine.SearcherScope.INTERNAL;

public class DataFusionReaderManagerTests extends OpenSearchTestCase {
    private DataFusionService service;

    @Mock
    private Environment mockEnvironment;

    @Before
    public void setup() {
        MockitoAnnotations.openMocks(this);
        service = new DataFusionService(Collections.emptyMap());
        service.doStart();
    }

    // ========== Test Cases ==========

    /** Test that a reader is created with correct file count and cache pointer after initial refresh */
    public void testInitialReaderCreation() throws IOException {
        ShardPath shardPath = createShardPathWithResourceFiles("test-index", 0, "parquet_file_generation_0.parquet", "parquet_file_generation_1.parquet");
        DatafusionEngine engine = new DatafusionEngine(DataFormat.PARQUET, Collections.emptyList(), service, shardPath);
        DatafusionReaderManager readerManager = engine.getReferenceManager(INTERNAL);

        RefreshResult refreshResult = new RefreshResult();
        WriterFileSet writerFileSet = new WriterFileSet(shardPath.getDataPath(), 1);
        writerFileSet.add(shardPath.getDataPath() + "/parquet_file_generation_0.parquet");
        writerFileSet.add(shardPath.getDataPath() + "/parquet_file_generation_1.parquet");

        refreshResult.add(getMockDataFormat(), List.of(writerFileSet));
        readerManager.afterRefresh(true, new CatalogSnapshot(refreshResult, 1));

        DatafusionSearcher searcher = engine.acquireSearcher("test");
        DatafusionReader reader = searcher.getReader();
        // Assert RefCount 2 -> 1 for latest catalogSnapshot holder, 1 for search
        assertEquals(2,getRefCount(reader));

        assertEquals(2, reader.files.stream().toList().get(0).getFiles().size());
        assertNotEquals(-1, reader.cachePtr);

        searcher.close();
        // Assert RefCount 1 -> 1 for latest catalogSnapshot holder
        assertEquals(1, getRefCount(reader));
        reader.close();
        assertEquals(-1, reader.getCachePtr());
    }

    /** Test that multiple searchers share the same reader instance for efficiency */
    public void testMultipleSearchersShareSameReader() throws IOException {
        ShardPath shardPath = createShardPathWithResourceFiles("test-index", 0, "parquet_file_generation_0.parquet");
        DatafusionEngine engine = new DatafusionEngine(DataFormat.PARQUET, Collections.emptyList(), service, shardPath);
        DatafusionReaderManager readerManager = engine.getReferenceManager(INTERNAL);

        RefreshResult refreshResult = new RefreshResult();
        WriterFileSet writerFileSet = new WriterFileSet(shardPath.getDataPath(), 1);
        writerFileSet.add(shardPath.getDataPath() + "/parquet_file_generation_0.parquet");

        refreshResult.add(getMockDataFormat(), List.of(writerFileSet));
        readerManager.afterRefresh(true, new CatalogSnapshot(refreshResult, 1));

        DatafusionSearcher searcher1 = engine.acquireSearcher("test1");
        DatafusionSearcher searcher2 = engine.acquireSearcher("test2");

        DatafusionReader reader = searcher1.getReader();
        // Both searchers should share the same reader instance
        assertSame(searcher1.getReader(), searcher2.getReader());

        searcher1.close();
        assertEquals(2, getRefCount(reader));
        searcher2.close();
        assertEquals(1, getRefCount(reader));
        reader.decRef();
        assertEquals(0,getRefCount(reader));
        assertEquals(-1, reader.getCachePtr());
    }

    /** Test that reader stays alive when only some searchers are closed (reference counting) */
    public void testReaderSurvivesPartialSearcherClose() throws IOException {
        ShardPath shardPath = createShardPathWithResourceFiles("test-index", 0, "parquet_file_generation_0.parquet");
        DatafusionEngine engine = new DatafusionEngine(DataFormat.PARQUET, Collections.emptyList(), service, shardPath);
        DatafusionReaderManager readerManager = engine.getReferenceManager(INTERNAL);

        RefreshResult refreshResult = new RefreshResult();
        WriterFileSet writerFileSet = new WriterFileSet(shardPath.getDataPath(), 1);
        writerFileSet.add(shardPath.getDataPath() + "/parquet_file_generation_0.parquet");

        refreshResult.add(getMockDataFormat(), List.of(writerFileSet));
        readerManager.afterRefresh(true, new CatalogSnapshot(refreshResult, 1));

        DatafusionSearcher searcher1 = engine.acquireSearcher("test1");
        DatafusionSearcher searcher2 = engine.acquireSearcher("test2");
        DatafusionReader reader = searcher1.getReader();

        // Close first searcher - reader should stay alive
        searcher1.close();
        assertEquals(2,getRefCount(reader));
        assertNotEquals(-1, reader.cachePtr);

        // Close second searcher - reader should not be closed
        searcher2.close();
        assertEquals(1,getRefCount(reader));
        assertNotEquals(-1, reader.cachePtr);
    }

    /** Test that refresh creates a new reader with updated file list */
    public void testRefreshCreatesNewReader() throws IOException {
        ShardPath shardPath = createShardPathWithResourceFiles("test-index", 0, "parquet_file_generation_2.parquet");
        DatafusionEngine engine = new DatafusionEngine(DataFormat.PARQUET, Collections.emptyList(), service, shardPath);
        DatafusionReaderManager readerManager = engine.getReferenceManager(INTERNAL);

        // Initial refresh
        RefreshResult refreshResult1 = new RefreshResult();
        WriterFileSet writerFileSet1 = new WriterFileSet(shardPath.getDataPath(), 1);
        addFilesToShardPath(shardPath, "parquet_file_generation_0.parquet");
        writerFileSet1.add(shardPath.getDataPath() + "/parquet_file_generation_0.parquet");
        refreshResult1.add(getMockDataFormat(), List.of(writerFileSet1));
        readerManager.afterRefresh(true, new CatalogSnapshot(refreshResult1, 1));

        DatafusionSearcher searcher1 = engine.acquireSearcher("test1");
        DatafusionReader reader1 = searcher1.getReader();
        assertEquals(2, getRefCount(reader1));

        // Add new file and refresh
        addFilesToShardPath(shardPath, "parquet_file_generation_1.parquet");
        RefreshResult refreshResult2 = new RefreshResult();
        WriterFileSet writerFileSet2 = new WriterFileSet(shardPath.getDataPath(), 2);
        writerFileSet2.add(shardPath.getDataPath() + "/parquet_file_generation_0.parquet");
        writerFileSet2.add(shardPath.getDataPath() + "/parquet_file_generation_1.parquet");
        refreshResult2.add(getMockDataFormat(), List.of(writerFileSet2));
        readerManager.afterRefresh(true, new CatalogSnapshot(refreshResult2, 2));

        DatafusionSearcher searcher2 = engine.acquireSearcher("test2");
        DatafusionReader reader2 = searcher2.getReader();

        // Check refCount of initial Reader
        assertEquals(1, getRefCount(reader1));
        assertEquals(2, getRefCount(reader2));

        // Should have different readers
        assertNotSame(reader1, reader2);
        assertEquals(1, reader1.files.stream().toList().getFirst().getFiles().size());
        assertEquals(2, reader2.files.stream().toList().getFirst().getFiles().size());

        searcher1.close();
        assertEquals(0, getRefCount(reader1));
        searcher2.close();
        assertEquals(1, getRefCount(reader2));
    }

    /** Test that calling decRef on an already closed reader throws IllegalStateException */
    public void testDecRefAfterCloseThrowsException() throws IOException {
        ShardPath shardPath = createShardPathWithResourceFiles("test-index", 0, "parquet_file_generation_2.parquet");
        DatafusionEngine engine = new DatafusionEngine(DataFormat.PARQUET, Collections.emptyList(), service, shardPath);
        DatafusionReaderManager readerManager = engine.getReferenceManager(INTERNAL);

        RefreshResult refreshResult = new RefreshResult();
        WriterFileSet writerFileSet = new WriterFileSet(shardPath.getDataPath(), 1);
        writerFileSet.add(shardPath.getDataPath() + "/parquet_file_generation_2.parquet");
        refreshResult.add(getMockDataFormat(), List.of(writerFileSet));
        readerManager.afterRefresh(true, new CatalogSnapshot(refreshResult, 1));

        DatafusionSearcher searcher = engine.acquireSearcher("test");
        DatafusionReader reader = searcher.getReader();

        searcher.close();
        reader.decRef();
        assertEquals(-1, reader.cachePtr);

        // Calling decRef on closed reader should throw
        assertThrows(IllegalStateException.class, reader::decRef);
    }

// R1 -> f1,f2,f3
    // S1 -> f1, f2, f3
    // S2 -> f1, f2, f3
    //R2 -> f2,f3
    //S3 -> f2,f3

    public void testReaderClosesAfterSearchRelease() throws IOException {
        Map<String, Object[]> finalRes = new HashMap<>();
        DatafusionSearcher datafusionSearcher = null;

        ShardPath shardPath = createShardPathWithResourceFiles("test-index", 0, "parquet_file_generation_2.parquet", "parquet_file_generation_1.parquet");

        try {
            DatafusionEngine engine = new DatafusionEngine(DataFormat.PARQUET, Collections.emptyList(), service, shardPath);
            DatafusionReaderManager readerManager = engine.getReferenceManager(INTERNAL);

            RefreshResult refreshResult = new RefreshResult();
            WriterFileSet writerFileSet = new WriterFileSet(shardPath.getDataPath(), 1);
            writerFileSet.add(shardPath.getDataPath() + "/parquet_file_generation_2.parquet");
            writerFileSet.add(shardPath.getDataPath() + "/parquet_file_generation_1.parquet");
            List<WriterFileSet> writerFiles = List.of(writerFileSet);

            refreshResult.add(getMockDataFormat(), writerFiles);
            readerManager.afterRefresh(true, new CatalogSnapshot(refreshResult, 1));

            // DatafusionReader readerR1 = readerManager.acquire();
            DatafusionSearcher datafusionSearcherS1 = engine.acquireSearcher("Search");
            DatafusionReader readerR1 = datafusionSearcherS1.getReader();
            assertEquals(readerR1.files.size(), datafusionSearcherS1.getReader().files.size());

            DatafusionSearcher datafusionSearcher1v2 = engine.acquireSearcher("Search");
            DatafusionReader readerR1v2 = datafusionSearcher1v2.getReader();
            assertEquals(readerR1v2.files.size(), datafusionSearcher1v2.getReader().files.size());

            // Check if same reader is referenced by both Searches
            assertEquals(readerR1v2, readerR1);

            addFilesToShardPath(shardPath, "parquet_file_generation_0.parquet");
            // now trigger refresh to have new Reader with F2, F3
            RefreshResult refreshResultR2 = new RefreshResult();
            WriterFileSet writerFileSet2 = new WriterFileSet(shardPath.getDataPath(), 2);
            writerFileSet2.add(shardPath.getDataPath() + "/parquet_file_generation_1.parquet");
            writerFileSet2.add(shardPath.getDataPath() + "/parquet_file_generation_0.parquet");
            List<WriterFileSet> writerFiles2 = List.of(writerFileSet2);

            refreshResultR2.add(getMockDataFormat(), writerFiles2);
            readerManager.afterRefresh(true, new CatalogSnapshot(refreshResultR2, 2));

            // now check if new Reader is created with F2, F3
            // DatafusionReader readerR2 = readerManager.acquire();
            DatafusionSearcher datafusionSearcherS2 = engine.acquireSearcher("Search");
            DatafusionReader readerR2 = datafusionSearcherS2.getReader();
            assertEquals(readerR2.files.size(), datafusionSearcherS2.getReader().files.size());

            //now we close S1 and automatically R1 will be closed
            datafusionSearcherS1.close();
            // 1 for SearcherS1v2
            assertEquals(1, getRefCount(readerR1));
            // 1 for SearcherS2 and 1 for CatalogSnapshot
            assertEquals(2, getRefCount(readerR2));
            assertNotEquals(-1, readerR1.cachePtr);
            datafusionSearcher1v2.close();
            assertEquals(-1, readerR1v2.cachePtr);

            assertThrows(IllegalStateException.class, () -> readerR1.decRef());
            datafusionSearcherS2.close();
            assertEquals(1, getRefCount(readerR2));
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (datafusionSearcher != null) {
                datafusionSearcher.close();
            }
        }
    }

    /** Test end-to-end search functionality with substrait plan execution and result verification */
    public void testSearch() throws IOException {

        ShardPath shardPath = createShardPathWithResourceFiles("index-7", 0, "parquet_file_generation_0.parquet");
        DatafusionEngine engine = new DatafusionEngine(DataFormat.PARQUET, Collections.emptyList(), service, shardPath);
        DatafusionReaderManager readerManager = engine.getReferenceManager(INTERNAL);

        // Initial refresh
        RefreshResult refreshResult1 = new RefreshResult();
        WriterFileSet writerFileSet1 = new WriterFileSet(shardPath.getDataPath(), 1);
        writerFileSet1.add(shardPath.getDataPath() + "/parquet_file_generation_0.parquet");
        refreshResult1.add(getMockDataFormat(), List.of(writerFileSet1));
        readerManager.afterRefresh(true, new CatalogSnapshot(refreshResult1, 1));

        DatafusionSearcher searcher1 = engine.acquireSearcher("search");
        DatafusionReader reader1 = searcher1.getReader();

        byte[] protoContent;

        try (InputStream is = getClass().getResourceAsStream("/substrait_plan_test.pb")) {
            protoContent = is.readAllBytes();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        DatafusionQuery datafusionQuery = new DatafusionQuery("index-7", protoContent, new java.util.ArrayList<>());
        Map<String, Long> expectedResults = new HashMap<>();
        expectedResults.put("min", 2L);
        expectedResults.put("max", 4L);
        expectedResults.put("count()", 2L);

        verifySearchResults(searcher1,datafusionQuery,expectedResults);

        logger.info("AFTER REFRESH");

        addFilesToShardPath(shardPath, "parquet_file_generation_1.parquet");
        RefreshResult refreshResult2 = new RefreshResult();
        WriterFileSet writerFileSet2 = new WriterFileSet(shardPath.getDataPath(), 2);
        writerFileSet2.add(shardPath.getDataPath() + "/parquet_file_generation_1.parquet");
        refreshResult2.add(getMockDataFormat(), List.of(writerFileSet2));
        readerManager.afterRefresh(true, new CatalogSnapshot(refreshResult2, 2));

        expectedResults = new HashMap<>();
        expectedResults.put("min", 3L);
        expectedResults.put("max", 8L);
        expectedResults.put("count()", 2L);

        DatafusionSearcher searcher2 = engine.acquireSearcher("test2");
        verifySearchResults(searcher2,datafusionQuery,expectedResults);

        DatafusionReader reader2 = searcher2.getReader();

        // Should have different readers
        assertNotSame(reader1, reader2);
        assertEquals(1, reader1.files.stream().toList().getFirst().getFiles().size());
        assertEquals(1, reader2.files.stream().toList().getFirst().getFiles().size());

        searcher1.close();
        assertEquals(-1,reader1.cachePtr);
        searcher2.close();
    }

    // ========== Helper Methods ==========

    private int getRefCount(DatafusionReader reader) {
        try {
            java.lang.reflect.Field refCountField = DatafusionReader.class.getDeclaredField("refCount");
            refCountField.setAccessible(true);
            return ((AtomicInteger) refCountField.get(reader)).get();
        } catch (Exception e) {
            throw new RuntimeException("Failed to get ref count", e);
        }
    }

    private org.opensearch.index.engine.exec.DataFormat getMockDataFormat() {
        return new org.opensearch.index.engine.exec.DataFormat() {
            @Override
            public Setting<Settings> dataFormatSettings() { return null; }

            @Override
            public Setting<Settings> clusterLeveldataFormatSettings() { return null; }

            @Override
            public String name() { return "parquet"; }

            @Override
            public void configureStore() {}
        };
    }

    private ShardPath createCustomShardPath(String indexName, int shardId) {
        Index index = new Index(indexName, UUID.randomUUID().toString());
        ShardId shId = new ShardId(index, shardId);
        Path dataPath = createTempDir().resolve("indices").resolve(index.getUUID()).resolve(String.valueOf(shardId));
        return new ShardPath(false, dataPath, dataPath, shId);
    }

    private void addFilesToShardPath(ShardPath shardPath, String... fileNames) throws IOException {
        for (String resourceFileName : fileNames) {
            try (InputStream is = getClass().getResourceAsStream("/" + resourceFileName)) {
                Path targetPath = shardPath.getDataPath().resolve(resourceFileName);
                java.nio.file.Files.createDirectories(targetPath.getParent());
                if (is != null) {
                    java.nio.file.Files.copy(is, targetPath);
                } else {
                    java.nio.file.Files.createFile(targetPath);
                }
            }
        }
    }

    private ShardPath createShardPathWithResourceFiles(String indexName, int shardId, String... resourceFileNames) throws IOException {
        ShardPath shardPath = createCustomShardPath(indexName, shardId);

        for (String resourceFileName : resourceFileNames) {
            try (InputStream is = getClass().getResourceAsStream("/" + resourceFileName)) {
                Path targetPath = shardPath.getDataPath().resolve(resourceFileName);
                java.nio.file.Files.createDirectories(targetPath.getParent());
                if (is != null) {
                    java.nio.file.Files.copy(is, targetPath);
                } else {
                    java.nio.file.Files.createFile(targetPath);
                }
            }
        }

        return shardPath;
    }

    private void verifySearchResults(DatafusionSearcher searcher, DatafusionQuery datafusionQuery, Map<String, Long> expectedResults) throws IOException {
        Map<String, Object[]> finalRes = new HashMap<>();
        Long streamPointer = searcher.search(datafusionQuery, service.getTokioRuntimePointer());

        RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
        RecordBatchStream stream = new RecordBatchStream(streamPointer, service.getTokioRuntimePointer(), allocator);

        SearchResultsCollector<RecordBatchStream> collector = new SearchResultsCollector<RecordBatchStream>() {
            @Override
            public void collect(RecordBatchStream value) {
                VectorSchemaRoot root = value.getVectorSchemaRoot();
                for (Field field : root.getSchema().getFields()) {
                    String filedName = field.getName();
                    FieldVector fieldVector = root.getVector(filedName);
                    Object[] fieldValues = new Object[fieldVector.getValueCount()];
                    for (int i = 0; i < fieldVector.getValueCount(); i++) {
                        fieldValues[i] = fieldVector.getObject(i);
                    }
                    finalRes.put(filedName, fieldValues);
                }
            }
        };

        while (stream.loadNextBatch().join()) {
            collector.collect(stream);
        }

        for (Map.Entry<String, Object[]> entry : finalRes.entrySet()) {
            logger.info("{}: {}", entry.getKey(), java.util.Arrays.toString(entry.getValue()));
            assertEquals(Long.valueOf(entry.getValue()[0].toString()), expectedResults.get(entry.getKey()));
        }
    }

    private byte[] readSubstraitPlanFromResources(String fileName) throws IOException {
        try (InputStream is = getClass().getResourceAsStream("/" + fileName)) {
            if (is == null) {
                throw new IOException("Substrait plan file not found: " + fileName);
            }
            return is.readAllBytes();
        }
    }
}






