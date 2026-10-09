/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.be.datafusion.docvalues.ParquetSegmentBindings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.NativeStoreHandle;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link DatafusionReaderManager}.
 *
 * <p>These tests exercise the manager's field storage, null-safety, and
 * delegation logic without loading the native library. Since
 * {@link DatafusionReader} calls {@code NativeBridge} (FFM) in its constructor,
 * we test only the manager's handle storage and lifecycle methods that don't
 * create readers.
 */
public class DatafusionReaderManagerTests extends OpenSearchTestCase {

    private static final DataFormat TEST_FORMAT = new DataFormat() {
        @Override
        public String name() {
            return "parquet";
        }

        @Override
        public long priority() {
            return 1;
        }

        @Override
        public Set<FieldTypeCapabilities> supportedFields() {
            return Set.of();
        }
    };

    private ShardPath createTestShardPath() throws IOException {
        ShardId shardId = new ShardId(new Index("test-index", "test-uuid"), 0);
        Path tempDir = createTempDir("shard");
        // ShardPath requires the path to end with: <index-uuid>/<shard-id>
        Path dataPath = tempDir.resolve("test-uuid").resolve("0");
        Files.createDirectories(dataPath);
        return new ShardPath(false, dataPath, dataPath, shardId);
    }

    /**
     * Constructor should accept null handle (hot path where no native store is available).
     */
    public void testConstructorAcceptsNullHandle() throws IOException {
        ShardPath shardPath = createTestShardPath();
        DataFusionService mockService = mock(DataFusionService.class);

        DatafusionReaderManager manager = new DatafusionReaderManager(
            TEST_FORMAT,
            shardPath,
            mockService,
            null,
            List.of(),
            List.of(),
            new ParquetSegmentBindings()
        );
        assertNotNull(manager);
        manager.close();
    }

    /**
     * Constructor should accept EMPTY handle (equivalent to null for native store).
     */
    public void testConstructorAcceptsEmptyHandle() throws IOException {
        ShardPath shardPath = createTestShardPath();
        DataFusionService mockService = mock(DataFusionService.class);

        DatafusionReaderManager manager = new DatafusionReaderManager(
            TEST_FORMAT,
            shardPath,
            mockService,
            NativeStoreHandle.EMPTY,
            List.of(),
            List.of(),
            new ParquetSegmentBindings()
        );
        assertNotNull(manager);
        manager.close();
    }

    /**
     * close() with no readers should not throw.
     */
    public void testCloseWithNoReadersDoesNotThrow() throws IOException {
        ShardPath shardPath = createTestShardPath();
        DataFusionService mockService = mock(DataFusionService.class);

        DatafusionReaderManager manager = new DatafusionReaderManager(
            TEST_FORMAT,
            shardPath,
            mockService,
            null,
            List.of(),
            List.of(),
            new ParquetSegmentBindings()
        );
        // Should not throw — no readers to close
        manager.close();
    }

    /**
     * onFilesDeleted with null collection should not interact with service.
     */
    public void testOnFilesDeletedWithNullIsNoOp() throws IOException {
        ShardPath shardPath = createTestShardPath();
        DataFusionService mockService = mock(DataFusionService.class);

        DatafusionReaderManager manager = new DatafusionReaderManager(
            TEST_FORMAT,
            shardPath,
            mockService,
            null,
            List.of(),
            List.of(),
            new ParquetSegmentBindings()
        );
        manager.onFilesDeleted(null);
        verifyNoInteractions(mockService);
        manager.close();
    }

    /**
     * onFilesDeleted with empty collection should not interact with service.
     */
    public void testOnFilesDeletedWithEmptyCollectionIsNoOp() throws IOException {
        ShardPath shardPath = createTestShardPath();
        DataFusionService mockService = mock(DataFusionService.class);

        DatafusionReaderManager manager = new DatafusionReaderManager(
            TEST_FORMAT,
            shardPath,
            mockService,
            null,
            List.of(),
            List.of(),
            new ParquetSegmentBindings()
        );
        manager.onFilesDeleted(List.of());
        verifyNoInteractions(mockService);
        manager.close();
    }

    /**
     * onFilesAdded with null collection should not interact with service.
     */
    public void testOnFilesAddedWithNullIsNoOp() throws IOException {
        ShardPath shardPath = createTestShardPath();
        DataFusionService mockService = mock(DataFusionService.class);

        DatafusionReaderManager manager = new DatafusionReaderManager(
            TEST_FORMAT,
            shardPath,
            mockService,
            null,
            List.of(),
            List.of(),
            new ParquetSegmentBindings()
        );
        manager.onFilesAdded(null);
        verifyNoInteractions(mockService);
        manager.close();
    }

    /**
     * onFilesAdded with empty collection should not interact with service.
     */
    public void testOnFilesAddedWithEmptyCollectionIsNoOp() throws IOException {
        ShardPath shardPath = createTestShardPath();
        DataFusionService mockService = mock(DataFusionService.class);

        DatafusionReaderManager manager = new DatafusionReaderManager(
            TEST_FORMAT,
            shardPath,
            mockService,
            null,
            List.of(),
            List.of(),
            new ParquetSegmentBindings()
        );
        manager.onFilesAdded(List.of());
        verifyNoInteractions(mockService);
        manager.close();
    }

    /**
     * onFilesDeleted with non-empty collection should delegate to service with absolute paths.
     */
    public void testOnFilesDeletedDelegatesToService() throws IOException {
        ShardPath shardPath = createTestShardPath();
        DataFusionService mockService = mock(DataFusionService.class);

        DatafusionReaderManager manager = new DatafusionReaderManager(
            TEST_FORMAT,
            shardPath,
            mockService,
            null,
            List.of(),
            List.of(),
            new ParquetSegmentBindings()
        );
        Collection<String> files = List.of("seg_0.parquet", "seg_1.parquet");
        manager.onFilesDeleted(files);

        String expectedDir = shardPath.getDataPath().resolve("parquet").toString();
        Collection<String> expectedPaths = List.of(expectedDir + "/seg_0.parquet", expectedDir + "/seg_1.parquet");
        verify(mockService).onFilesDeleted(expectedPaths);
        manager.close();
    }

    /**
     * onFilesAdded with non-empty collection should delegate to service with absolute paths.
     */
    public void testOnFilesAddedDelegatesToService() throws IOException {
        ShardPath shardPath = createTestShardPath();
        DataFusionService mockService = mock(DataFusionService.class);

        DatafusionReaderManager manager = new DatafusionReaderManager(
            TEST_FORMAT,
            shardPath,
            mockService,
            null,
            List.of(),
            List.of(),
            new ParquetSegmentBindings()
        );
        Collection<String> files = List.of("seg_0.parquet");
        manager.onFilesAdded(files);

        String expectedDir = shardPath.getDataPath().resolve("parquet").toString();
        Collection<String> expectedPaths = List.of(expectedDir + "/seg_0.parquet");
        verify(mockService).onFilesAdded(expectedPaths);
        manager.close();
    }

    /**
     * beforeRefresh should not throw.
     */
    public void testBeforeRefreshDoesNotThrow() throws IOException {
        ShardPath shardPath = createTestShardPath();
        DataFusionService mockService = mock(DataFusionService.class);

        DatafusionReaderManager manager = new DatafusionReaderManager(
            TEST_FORMAT,
            shardPath,
            mockService,
            null,
            List.of(),
            List.of(),
            new ParquetSegmentBindings()
        );
        manager.beforeRefresh();
        manager.close();
    }

    /**
     * afterRefresh with didRefresh=false should not create a reader.
     */
    public void testAfterRefreshWithDidRefreshFalseIsNoOp() throws IOException {
        ShardPath shardPath = createTestShardPath();
        DataFusionService mockService = mock(DataFusionService.class);

        DatafusionReaderManager manager = new DatafusionReaderManager(
            TEST_FORMAT,
            shardPath,
            mockService,
            null,
            List.of(),
            List.of(),
            new ParquetSegmentBindings()
        );
        // didRefresh=false means no new data — should be a no-op
        manager.afterRefresh(false, null);
        manager.close();
    }

    /**
     * getReader before any afterRefresh has populated a reader must throw IOException, since no reader
     * is registered for the requested snapshot yet.
     */
    public void testGetReaderWithNoRefreshThrows() throws IOException {
        ShardPath shardPath = createTestShardPath();
        DataFusionService mockService = mock(DataFusionService.class);

        DatafusionReaderManager manager = new DatafusionReaderManager(
            TEST_FORMAT,
            shardPath,
            mockService,
            null,
            List.of(),
            List.of(),
            new ParquetSegmentBindings()
        );
        CatalogSnapshot snapshot = mock(CatalogSnapshot.class);
        when(snapshot.getId()).thenReturn(1L);
        expectThrows(IOException.class, () -> manager.getReader(snapshot));
        manager.close();
    }

    /**
     * afterRefresh must register a {@code generation -> Parquet path} binding built from the catalog
     * snapshot, so the doc-values codec can resolve the file without server-side SegmentInfo stamping.
     * The reader created alongside it needs the native library, which unit tests do not load; since
     * registration happens before reader creation, the binding is observable even when reader creation
     * fails, so that failure is tolerated here.
     */
    public void testAfterRefreshRegistersGenerationBinding() throws IOException {
        ShardPath shardPath = createTestShardPath();
        DataFusionService mockService = mock(DataFusionService.class);
        ParquetSegmentBindings bindings = new ParquetSegmentBindings();
        DatafusionReaderManager manager = new DatafusionReaderManager(
            TEST_FORMAT,
            shardPath,
            mockService,
            null,
            List.of(),
            List.of(),
            bindings
        );

        String parquetFile = "seg_5.parquet";
        CatalogSnapshot snapshot = snapshotWithParquetSegment(shardPath, 1L, 5L, parquetFile);
        refreshToleratingNativeReader(manager, snapshot);

        ParquetSegmentBindings.Binding binding = bindings.resolve(shardPath.getShardId(), 5L);
        assertNotNull("afterRefresh must register a binding for the snapshot's generation", binding);
        // Compare by string: the binding's path is rebuilt from the writer-file-set directory string
        // (Path.of(dir, file) -> a plain UnixPath), which is not .equals() to the test's mockfile FilterPath
        // even when they denote the same location. This mirrors the toString() comparisons used elsewhere here.
        assertEquals(shardPath.getDataPath().resolve("parquet").resolve(parquetFile).toString(), binding.parquetFile().toString());
        assertEquals("hot/local shard yields the LOCAL_STORE sentinel (0)", 0L, binding.storePointer());

        manager.close();
        assertNull("close must drop the shard's bindings", bindings.resolve(shardPath.getShardId(), 5L));
    }

    /**
     * onDeleted releases the deleted snapshot's bindings, but a generation still referenced by another
     * live snapshot of the same shard stays resolvable (retention rule), resolving newest-first.
     */
    public void testOnDeletedReleasesButLiveSnapshotKeepsGenerationResolvable() throws IOException {
        ShardPath shardPath = createTestShardPath();
        ShardId shardId = shardPath.getShardId();
        DataFusionService mockService = mock(DataFusionService.class);
        ParquetSegmentBindings bindings = new ParquetSegmentBindings();
        DatafusionReaderManager manager = new DatafusionReaderManager(
            TEST_FORMAT,
            shardPath,
            mockService,
            null,
            List.of(),
            List.of(),
            bindings
        );

        // Two live snapshots share generation 5 (e.g. an un-merged segment), each naming its own file.
        CatalogSnapshot older = snapshotWithParquetSegment(shardPath, 1L, 5L, "seg_5_old.parquet");
        CatalogSnapshot newer = snapshotWithParquetSegment(shardPath, 2L, 5L, "seg_5_new.parquet");
        refreshToleratingNativeReader(manager, older);
        refreshToleratingNativeReader(manager, newer);

        Path dir = shardPath.getDataPath().resolve("parquet");
        // newest-first: the newer snapshot wins while both are live.
        assertEquals(dir.resolve("seg_5_new.parquet").toString(), bindings.resolve(shardId, 5L).parquetFile().toString());

        // Deleting the newer snapshot leaves generation 5 resolvable through the older, still-live one.
        manager.onDeleted(newer);
        assertNotNull("generation kept alive by the older live snapshot", bindings.resolve(shardId, 5L));
        assertEquals(dir.resolve("seg_5_old.parquet").toString(), bindings.resolve(shardId, 5L).parquetFile().toString());

        // Deleting the last live snapshot referencing it finally drops the generation.
        manager.onDeleted(older);
        assertNull("no live snapshot references generation 5 anymore", bindings.resolve(shardId, 5L));

        manager.close();
    }

    /**
     * Drives afterRefresh, tolerating the native-reader construction that unit tests cannot perform.
     * Binding registration runs before reader creation, so it completes regardless.
     */
    private static void refreshToleratingNativeReader(DatafusionReaderManager manager, CatalogSnapshot snapshot) {
        try {
            manager.afterRefresh(true, snapshot);
        } catch (Exception nativeReaderUnavailable) {
            // Expected in a unit test with no native library; the binding was already registered.
        }
    }

    /** A single-segment catalog snapshot exposing one parquet {@link WriterFileSet} for {@code generation}. */
    private static CatalogSnapshot snapshotWithParquetSegment(ShardPath shardPath, long snapshotId, long generation, String parquetFile) {
        String parquetDir = shardPath.getDataPath().resolve("parquet").toString();
        WriterFileSet parquetWfs = new WriterFileSet(parquetDir, generation, Set.of(parquetFile), 1L, 1_000_000L);
        Segment segment = new Segment(generation, Map.of(TEST_FORMAT.name(), parquetWfs));
        CatalogSnapshot snapshot = mock(CatalogSnapshot.class);
        when(snapshot.getId()).thenReturn(snapshotId);
        when(snapshot.getSegments()).thenReturn(List.of(segment));
        when(snapshot.getSearchableFiles(TEST_FORMAT.name())).thenReturn(List.of());
        return snapshot;
    }
}
