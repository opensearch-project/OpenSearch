/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.NativeStoreHandle;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;

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

        DatafusionReaderManager manager = new DatafusionReaderManager(TEST_FORMAT, shardPath, mockService, null);
        assertNotNull(manager);
        manager.close();
    }

    /**
     * Constructor should accept EMPTY handle (equivalent to null for native store).
     */
    public void testConstructorAcceptsEmptyHandle() throws IOException {
        ShardPath shardPath = createTestShardPath();
        DataFusionService mockService = mock(DataFusionService.class);

        DatafusionReaderManager manager = new DatafusionReaderManager(TEST_FORMAT, shardPath, mockService, NativeStoreHandle.EMPTY);
        assertNotNull(manager);
        manager.close();
    }

    /**
     * close() with no readers should not throw.
     */
    public void testCloseWithNoReadersDoesNotThrow() throws IOException {
        ShardPath shardPath = createTestShardPath();
        DataFusionService mockService = mock(DataFusionService.class);

        DatafusionReaderManager manager = new DatafusionReaderManager(TEST_FORMAT, shardPath, mockService, null);
        // Should not throw — no readers to close
        manager.close();
    }

    /**
     * onFilesDeleted with null collection should not interact with service.
     */
    public void testOnFilesDeletedWithNullIsNoOp() throws IOException {
        ShardPath shardPath = createTestShardPath();
        DataFusionService mockService = mock(DataFusionService.class);

        DatafusionReaderManager manager = new DatafusionReaderManager(TEST_FORMAT, shardPath, mockService, null);
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

        DatafusionReaderManager manager = new DatafusionReaderManager(TEST_FORMAT, shardPath, mockService, null);
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

        DatafusionReaderManager manager = new DatafusionReaderManager(TEST_FORMAT, shardPath, mockService, null);
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

        DatafusionReaderManager manager = new DatafusionReaderManager(TEST_FORMAT, shardPath, mockService, null);
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

        DatafusionReaderManager manager = new DatafusionReaderManager(TEST_FORMAT, shardPath, mockService, null);
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

        DatafusionReaderManager manager = new DatafusionReaderManager(TEST_FORMAT, shardPath, mockService, null);
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

        DatafusionReaderManager manager = new DatafusionReaderManager(TEST_FORMAT, shardPath, mockService, null);
        manager.beforeRefresh();
        manager.close();
    }

    /**
     * afterRefresh with didRefresh=false should not create a reader.
     */
    public void testAfterRefreshWithDidRefreshFalseIsNoOp() throws IOException {
        ShardPath shardPath = createTestShardPath();
        DataFusionService mockService = mock(DataFusionService.class);

        DatafusionReaderManager manager = new DatafusionReaderManager(TEST_FORMAT, shardPath, mockService, null);
        // didRefresh=false means no new data — should be a no-op
        manager.afterRefresh(false, null);
        manager.close();
    }

    /**
     * getReader with no prior afterRefresh should throw IOException.
     */
    public void testGetReaderWithNoRefreshThrows() throws IOException {
        ShardPath shardPath = createTestShardPath();
        DataFusionService mockService = mock(DataFusionService.class);

        DatafusionReaderManager manager = new DatafusionReaderManager(TEST_FORMAT, shardPath, mockService, null);
        expectThrows(IllegalArgumentException.class, () -> manager.getReader(null));
        manager.close();
    }
}
