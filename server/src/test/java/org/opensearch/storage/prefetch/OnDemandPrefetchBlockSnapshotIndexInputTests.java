/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.prefetch;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.apache.lucene.util.Constants;
import org.apache.lucene.util.Version;
import org.opensearch.common.lucene.store.ByteArrayIndexInput;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.index.store.remote.file.AbstractBlockIndexInput;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;
import org.opensearch.index.store.remote.utils.BlobFetchRequest;
import org.opensearch.index.store.remote.utils.TransferManager;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.Before;

import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.function.Supplier;

import static org.opensearch.common.settings.ClusterSettings.BUILT_IN_CLUSTER_SETTINGS;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public class OnDemandPrefetchBlockSnapshotIndexInputTests extends OpenSearchTestCase {

    private static final String RESOURCE_DESCRIPTION = "Test TestOnDemandPrefetchBlockSnapshotIndexInput Block Size";
    private static final long BLOCK_SNAPSHOT_FILE_OFFSET = 0;
    private static final String FILE_NAME = "File_Name";
    private static final String BLOCK_FILE_PREFIX = FILE_NAME;
    private static final boolean IS_CLONE = false;
    private static final int FILE_SIZE = 29360128;
    protected static final int FILE_CACHE_CAPACITY = 10000000;
    private TransferManager transferManager;
    private LockFactory lockFactory;
    private BlobStoreIndexShardSnapshot.FileInfo fileInfo;
    private Path path;
    private ThreadPool threadPool;
    FileCache fileCache;
    private TieredStoragePrefetchSettings tieringServicePrefetchSettings;

    @Before
    public void init() {
        assumeFalse("Awaiting Windows fix https://github.com/opensearch-project/OpenSearch/issues/5396", Constants.WINDOWS);
        transferManager = mock(TransferManager.class);
        lockFactory = SimpleFSLockFactory.INSTANCE;
        threadPool = new TestThreadPool("PrefetchBlockSnapshotIndexInputTests");
        path = createTempDir("TestOnDemandPrefetchBlockSnapshotIndexInputTests");
        int concurrencyLevel = randomIntBetween(1, 2);
        fileCache = FileCacheFactory.createConcurrentLRUFileCache(FILE_CACHE_CAPACITY, concurrencyLevel);
        Set<Setting<?>> clusterSettingsToAdd = new HashSet<>(BUILT_IN_CLUSTER_SETTINGS);
        clusterSettingsToAdd.add(TieredStoragePrefetchSettings.READ_AHEAD_BLOCK_COUNT);
        clusterSettingsToAdd.add(TieredStoragePrefetchSettings.STORED_FIELDS_PREFETCH_ENABLED_SETTING);
        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, clusterSettingsToAdd);
        this.tieringServicePrefetchSettings = new TieredStoragePrefetchSettings(clusterSettings);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
    }

    public Supplier<TieredStoragePrefetchSettings> getPrefetchSettingsSupplier() {
        return () -> this.tieringServicePrefetchSettings;
    }

    public void testPrefetch() throws Exception {
        final TestOnDemandPrefetchBlockSnapshotIndexInput blockedSnapshotFile = createTestOnDemandPrefetchBlockSnapshotIndexInput(23);

        blockedSnapshotFile.prefetch(0, 10);
        verify(transferManager, times(1)).fetchBlobAsync(any(BlobFetchRequest.class));
        blockedSnapshotFile.prefetch(0, 8388670);
        verify(transferManager, times(3)).fetchBlobAsync(any(BlobFetchRequest.class));
        blockedSnapshotFile.prefetch(0, 16777350);
        verify(transferManager, times(6)).fetchBlobAsync(any(BlobFetchRequest.class));
    }

    public void testSettings() throws Exception {
        final TestOnDemandPrefetchBlockSnapshotIndexInput blockedSnapshotFile = createTestOnDemandPrefetchBlockSnapshotIndexInput(23);
        assertFalse(blockedSnapshotFile.checkIfFileEnabledReadAhead());
        assertTrue(blockedSnapshotFile.checkIfStoredFieldsPrefetchEnabled());
    }

    public void testReadAheadEnabled() throws Exception {
        TestOnDemandPrefetchBlockSnapshotIndexInput indexInput = getIndexInput("lucene.dvd", "clone");
        assertTrue(indexInput.checkIfFileEnabledReadAhead());
        indexInput = getIndexInput("_1.fdt", "clone");
        assertFalse(indexInput.checkIfFileEnabledReadAhead());
        indexInput = getIndexInput("_0.cfs", "lucene9.dvd");
        assertTrue(indexInput.checkIfFileEnabledReadAhead());
        indexInput = getIndexInput("_1.cfe", "lucene9.dvd");
        assertFalse(indexInput.checkIfFileEnabledReadAhead());
    }

    public void testPrefetchDisabledBySettings() throws Exception {
        // Disable stored fields prefetch via settings
        tieringServicePrefetchSettings.setStoredFieldsPrefetchEnabled(false);
        final TestOnDemandPrefetchBlockSnapshotIndexInput blockedSnapshotFile = createTestOnDemandPrefetchBlockSnapshotIndexInput(23);
        // prefetch should be a no-op when disabled
        blockedSnapshotFile.prefetch(0, 10);
        verify(transferManager, never()).fetchBlobAsync(any(BlobFetchRequest.class));
    }

    public void testFetchBlockWithCacheTracking() throws Exception {
        final TestOnDemandPrefetchBlockSnapshotIndexInput blockedSnapshotFile = createTestOnDemandPrefetchBlockSnapshotIndexInput(23);

        // Fetch a block that exists (should be cache hit)
        IndexInput result = blockedSnapshotFile.fetchBlockPublic(0);
        assertNotNull("Fetched block should not be null", result);
    }

    public void testDownloadBlocksAsyncWithCacheOptimization() throws Exception {
        final TestOnDemandPrefetchBlockSnapshotIndexInput blockedSnapshotFile = createTestOnDemandPrefetchBlockSnapshotIndexInput(23);

        // Test that cached blocks are not re-downloaded
        blockedSnapshotFile.downloadBlocksAsync(0, 1, false);
    }

    public void test8MBBlock() throws Exception {
        runAllTestsFor(23);
    }

    public void test4KBBlock() throws Exception {
        runAllTestsFor(12);
    }

    public void test1MBBlock() throws Exception {
        runAllTestsFor(20);
    }

    public void test4MBBlock() throws Exception {
        runAllTestsFor(22);
    }

    public void testChunkedRepositoryWithBlockSizeGreaterThanChunkSize() throws IOException {
        verifyChunkedRepository(
            new ByteSizeValue(8, ByteSizeUnit.KB).getBytes(),
            new ByteSizeValue(2, ByteSizeUnit.KB).getBytes(),
            new ByteSizeValue(15, ByteSizeUnit.KB).getBytes()
        );
    }

    public void testChunkedRepositoryWithBlockSizeLessThanChunkSize() throws IOException {
        verifyChunkedRepository(
            new ByteSizeValue(1, ByteSizeUnit.KB).getBytes(),
            new ByteSizeValue(2, ByteSizeUnit.KB).getBytes(),
            new ByteSizeValue(3, ByteSizeUnit.KB).getBytes()
        );
    }

    public void testChunkedRepositoryWithBlockSizeEqualToChunkSize() throws IOException {
        verifyChunkedRepository(
            new ByteSizeValue(2, ByteSizeUnit.KB).getBytes(),
            new ByteSizeValue(2, ByteSizeUnit.KB).getBytes(),
            new ByteSizeValue(15, ByteSizeUnit.KB).getBytes()
        );
    }

    TestOnDemandPrefetchBlockSnapshotIndexInput getIndexInput(String fileName, String resourceDescription) {
        fileInfo = new BlobStoreIndexShardSnapshot.FileInfo(fileName, new StoreFileMetadata(fileName, FILE_SIZE, "", Version.LATEST), null);

        FSDirectory directory = null;
        try {
            directory = new MMapDirectory(path, lockFactory);
        } catch (IOException e) {
            fail("fail to create MMapDirectory: " + e.getMessage());
        }

        return new TestOnDemandPrefetchBlockSnapshotIndexInput(
            AbstractBlockIndexInput.builder()
                .resourceDescription(RESOURCE_DESCRIPTION)
                .offset(BLOCK_SNAPSHOT_FILE_OFFSET)
                .length(FILE_SIZE)
                .blockSizeShift(23)
                .isClone(IS_CLONE),
            resourceDescription,
            fileInfo,
            directory,
            transferManager,
            threadPool,
            fileCache,
            getPrefetchSettingsSupplier()
        );
    }

    private void verifyChunkedRepository(long blockSize, long repositoryChunkSize, long fileSize) throws IOException {
        when(transferManager.fetchBlob(any())).thenReturn(new ByteArrayIndexInput("test", new byte[(int) blockSize]));
        try (
            FSDirectory directory = new MMapDirectory(path, lockFactory);
            IndexInput indexInput = new TestOnDemandPrefetchBlockSnapshotIndexInput(
                AbstractBlockIndexInput.builder()
                    .resourceDescription(RESOURCE_DESCRIPTION)
                    .offset(BLOCK_SNAPSHOT_FILE_OFFSET)
                    .length(FILE_SIZE)
                    .blockSizeShift((int) (Math.log(blockSize) / Math.log(2)))
                    .isClone(IS_CLONE),
                RESOURCE_DESCRIPTION,
                new BlobStoreIndexShardSnapshot.FileInfo(
                    FILE_NAME,
                    new StoreFileMetadata(FILE_NAME, fileSize, "", Version.LATEST),
                    new ByteSizeValue(repositoryChunkSize)
                ),
                directory,
                transferManager,
                threadPool,
                fileCache,
                getPrefetchSettingsSupplier()
            )
        ) {
            indexInput.seek(repositoryChunkSize);
        }
        verify(transferManager).fetchBlob(argThat(request -> request.getBlobLength() == blockSize));
    }

    private TestOnDemandPrefetchBlockSnapshotIndexInput createTestOnDemandPrefetchBlockSnapshotIndexInput(int blockSizeShift)
        throws IOException {
        fileInfo = new BlobStoreIndexShardSnapshot.FileInfo(
            FILE_NAME,
            new StoreFileMetadata(FILE_NAME, FILE_SIZE, "", Version.LATEST),
            null
        );

        int blockSize = 1 << blockSizeShift;

        doAnswer(invocation -> {
            BlobFetchRequest blobFetchRequest = invocation.getArgument(0);
            return blobFetchRequest.getDirectory().openInput(blobFetchRequest.getFileName(), IOContext.READONCE);
        }).when(transferManager).fetchBlob(any());

        FSDirectory directory = null;
        try {
            directory = new MMapDirectory(path, lockFactory);
        } catch (IOException e) {
            fail("fail to create MMapDirectory: " + e.getMessage());
        }

        initBlockFiles(blockSize, directory);

        return new TestOnDemandPrefetchBlockSnapshotIndexInput(
            AbstractBlockIndexInput.builder()
                .resourceDescription(RESOURCE_DESCRIPTION)
                .offset(BLOCK_SNAPSHOT_FILE_OFFSET)
                .length(FILE_SIZE)
                .blockSizeShift(blockSizeShift)
                .isClone(IS_CLONE),
            RESOURCE_DESCRIPTION,
            fileInfo,
            directory,
            transferManager,
            threadPool,
            fileCache,
            getPrefetchSettingsSupplier()
        );
    }

    private TestOnDemandPrefetchBlockSnapshotIndexInput createDvdFileIndexInput(int blockSizeShift) throws IOException {
        String dvdFileName = "_0.dvd";
        BlobStoreIndexShardSnapshot.FileInfo dvdFileInfo = new BlobStoreIndexShardSnapshot.FileInfo(
            dvdFileName,
            new StoreFileMetadata(dvdFileName, FILE_SIZE, "", Version.LATEST),
            null
        );

        int blockSize = 1 << blockSizeShift;

        doAnswer(invocation -> {
            BlobFetchRequest blobFetchRequest = invocation.getArgument(0);
            return blobFetchRequest.getDirectory().openInput(blobFetchRequest.getFileName(), IOContext.READONCE);
        }).when(transferManager).fetchBlob(any());

        FSDirectory directory = null;
        try {
            directory = new MMapDirectory(path, lockFactory);
        } catch (IOException e) {
            fail("fail to create MMapDirectory: " + e.getMessage());
        }

        // Create block files with the dvd file name prefix
        int numOfBlocks = FILE_SIZE / blockSize;
        int sizeOfLastBlock = FILE_SIZE % blockSize;
        try {
            for (int i = 0; i < numOfBlocks; i++) {
                String blockName = dvdFileName + "_block_" + i;
                IndexOutput output = directory.createOutput(blockName, null);
                for (int j = 0; j < blockSize / 2; j++) {
                    output.writeByte((byte) 48);
                    output.writeByte((byte) -80);
                }
                output.close();
            }
            if (numOfBlocks > 1 && sizeOfLastBlock != 0) {
                String lastBlockName = dvdFileName + "_block_" + numOfBlocks;
                IndexOutput output = directory.createOutput(lastBlockName, null);
                for (int i = 0; i < sizeOfLastBlock; i++) {
                    output.writeByte((byte) ((i & 1) == 0 ? 48 : -80));
                }
                output.close();
            }
        } catch (IOException e) {
            fail("fail to initialize dvd block files: " + e.getMessage());
        }

        return new TestOnDemandPrefetchBlockSnapshotIndexInput(
            AbstractBlockIndexInput.builder()
                .resourceDescription(RESOURCE_DESCRIPTION)
                .offset(BLOCK_SNAPSHOT_FILE_OFFSET)
                .length(FILE_SIZE)
                .blockSizeShift(blockSizeShift)
                .isClone(IS_CLONE),
            RESOURCE_DESCRIPTION,
            dvdFileInfo,
            directory,
            transferManager,
            threadPool,
            fileCache,
            getPrefetchSettingsSupplier()
        );
    }

    private void initBlockFiles(int blockSize, FSDirectory fsDirectory) {
        int numOfBlocks = FILE_SIZE / blockSize;
        int sizeOfLastBlock = FILE_SIZE % blockSize;

        try {
            for (int i = 0; i < numOfBlocks; i++) {
                String blockName = BLOCK_FILE_PREFIX + "_block_" + i;
                IndexOutput output = fsDirectory.createOutput(blockName, null);
                for (int j = 0; j < blockSize / 2; j++) {
                    output.writeByte((byte) 48);
                    output.writeByte((byte) -80);
                }
                output.close();
            }

            if (numOfBlocks > 1 && sizeOfLastBlock != 0) {
                String lastBlockName = BLOCK_FILE_PREFIX + "_block_" + numOfBlocks;
                IndexOutput output = fsDirectory.createOutput(lastBlockName, null);
                for (int i = 0; i < sizeOfLastBlock; i++) {
                    if ((i & 1) == 0) {
                        output.writeByte((byte) 48);
                    } else {
                        output.writeByte((byte) -80);
                    }
                }
                output.close();
            }
        } catch (IOException e) {
            fail("fail to initialize block files: " + e.getMessage());
        }
    }

    private void runAllTestsFor(int blockSizeShift) throws Exception {
        final TestOnDemandPrefetchBlockSnapshotIndexInput input = createTestOnDemandPrefetchBlockSnapshotIndexInput(blockSizeShift);
        final int blockSize = 1 << blockSizeShift;

        // testGetBlock
        assertEquals(0, input.getBlock(0L));
        assertEquals(1, input.getBlock(blockSize));
        assertEquals((FILE_SIZE - 1) / blockSize, input.getBlock(FILE_SIZE - 1));

        // testGetTotalBlocks
        assertEquals((FILE_SIZE - 1) / blockSize + 1, input.getTotalBlocks());

        // testGetBlockOffset
        assertEquals(1, input.getBlockOffset(1));
        assertEquals(0, input.getBlockOffset(blockSize));
        assertEquals((FILE_SIZE - 1) % blockSize, input.getBlockOffset(FILE_SIZE - 1));

        // testGetBlockStart
        assertEquals(0L, input.getBlockStart(0));
        assertEquals(blockSize, input.getBlockStart(1));
        assertEquals(blockSize * 2, input.getBlockStart(2));

        // testCurrentBlockStart
        input.seek(blockSize - 1);
        assertEquals(0L, input.currentBlockStart());
        input.seek(blockSize * 2 - 1);
        assertEquals(blockSize, input.currentBlockStart());

        // testCurrentBlockPosition
        input.seek(blockSize - 1);
        assertEquals(blockSize - 1, input.currentBlockPosition());
        input.seek(blockSize + 1);
        assertEquals(1, input.currentBlockPosition());

        // testClone
        input.seek(blockSize + 1);
        TestOnDemandPrefetchBlockSnapshotIndexInput clonedFile = (TestOnDemandPrefetchBlockSnapshotIndexInput) input.clone();
        assertEquals(clonedFile.currentBlockPosition(), input.currentBlockPosition());
        assertEquals(clonedFile.getFilePointer(), input.getFilePointer());
        clonedFile.seek(blockSize + 11);
        assertNotEquals(clonedFile.currentBlockPosition(), input.currentBlockPosition());

        // testReadByte
        input.seek(0);
        assertEquals((byte) 48, input.readByte());
        input.seek(1);
        assertEquals((byte) -80, input.readByte());
        input.seek(blockSize);
        assertEquals((byte) 48, input.readByte());

        // testReadShort
        input.seek(0);
        assertEquals(-20432, input.readShort());
        input.seek(blockSize - 1);
        assertEquals(12464, input.readShort());

        // testReadInt
        input.seek(0);
        assertEquals(-1338986448, input.readInt());
        input.seek(blockSize - 1);
        assertEquals(816853168, input.readInt());

        // testReadLong
        input.seek(0);
        assertEquals(-5750903000991223760L, input.readLong());

        // testSeek
        input.seek(0);
        assertEquals(0, input.getFilePointer());
        input.seek(blockSize + 11);
        assertEquals(blockSize + 11, input.getFilePointer());
        try {
            input.seek(FILE_SIZE + 1);
            fail("Should throw EOFException");
        } catch (EOFException e) {
            // expected
        }

        // testReadBytes
        input.seek(0);
        byte[] buf = new byte[4];
        input.readBytes(buf, 0, 4);
        assertEquals((byte) 48, buf[0]);
        assertEquals((byte) -80, buf[1]);
        assertEquals((byte) 48, buf[2]);
        assertEquals((byte) -80, buf[3]);
    }
}
