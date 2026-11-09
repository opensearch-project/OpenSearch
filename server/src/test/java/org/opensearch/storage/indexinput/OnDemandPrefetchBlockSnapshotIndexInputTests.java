/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.indexinput;

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
import org.opensearch.index.snapshots.blobstore.BlobStoreIndexShardSnapshot;
import org.opensearch.index.store.StoreFileMetadata;
import org.opensearch.index.store.remote.file.AbstractBlockIndexInput;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;
import org.opensearch.index.store.remote.utils.BlobFetchRequest;
import org.opensearch.index.store.remote.utils.TransferManager;
import org.opensearch.storage.prefetch.TieredStoragePrefetchSettings;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.Before;

import java.io.EOFException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Supplier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link OnDemandPrefetchBlockSnapshotIndexInput}.
 */
@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public class OnDemandPrefetchBlockSnapshotIndexInputTests extends OpenSearchTestCase {

    private static final String RESOURCE_DESCRIPTION = "Test OnDemandPrefetchBlockSnapshotIndexInput";
    private static final long BLOCK_SNAPSHOT_FILE_OFFSET = 0;
    private static final String FILE_NAME = "File_Name";
    private static final String BLOCK_FILE_PREFIX = FILE_NAME;
    private static final boolean IS_CLONE = false;
    private static final int FILE_SIZE = 29360128;
    private static final int FILE_CACHE_CAPACITY = 10000000;

    private TransferManager transferManager;
    private LockFactory lockFactory;
    private BlobStoreIndexShardSnapshot.FileInfo fileInfo;
    private Path path;
    private ThreadPool threadPool;
    private FileCache fileCache;

    private Supplier<TieredStoragePrefetchSettings> getPrefetchSettingsSupplier() {
        TieredStoragePrefetchSettings settings = mock(TieredStoragePrefetchSettings.class);
        when(settings.getReadAheadBlockCount()).thenReturn(TieredStoragePrefetchSettings.DEFAULT_READ_AHEAD_BLOCK_COUNT);
        when(settings.getReadAheadEnableFileFormats()).thenReturn(TieredStoragePrefetchSettings.READ_AHEAD_ENABLE_FILE_FORMATS);
        when(settings.isStoredFieldsPrefetchEnabled()).thenReturn(true);
        return () -> settings;
    }

    @Before
    public void init() {
        assumeFalse("Awaiting Windows fix", Constants.WINDOWS);
        transferManager = mock(TransferManager.class);
        lockFactory = SimpleFSLockFactory.INSTANCE;
        threadPool = new TestThreadPool("OnDemandPrefetchTests");
        path = createTempDir("TestOnDemandPrefetchBlockSnapshotIndexInputTests");
        int concurrencyLevel = randomIntBetween(1, 2);
        fileCache = FileCacheFactory.createConcurrentLRUFileCache(FILE_CACHE_CAPACITY, concurrencyLevel);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        threadPool.shutdownNow();
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

    public void testPrefetch() throws Exception {
        final OnDemandPrefetchBlockSnapshotIndexInput blockedSnapshotFile = createIndexInput(23);

        blockedSnapshotFile.prefetch(0, 10);
        verify(transferManager, times(1)).fetchBlobAsync(any(BlobFetchRequest.class));
        blockedSnapshotFile.prefetch(0, 8388670);
        verify(transferManager, times(3)).fetchBlobAsync(any(BlobFetchRequest.class));
        blockedSnapshotFile.prefetch(0, 16777350);
        verify(transferManager, times(6)).fetchBlobAsync(any(BlobFetchRequest.class));
    }

    public void testCloneAndSlice() throws Exception {
        final OnDemandPrefetchBlockSnapshotIndexInput input = createIndexInput(23);
        int blockSize = 1 << 23;

        input.seek(blockSize + 1);
        OnDemandPrefetchBlockSnapshotIndexInput cloned = input.clone();
        assertEquals(cloned.getFilePointer(), input.getFilePointer());
        cloned.seek(blockSize + 11);
        assertNotEquals(cloned.getFilePointer(), input.getFilePointer());

        IndexInput slice = input.slice("slice", blockSize - 11, 22);
        assertTrue(slice instanceof OnDemandPrefetchBlockSnapshotIndexInput);
        assertEquals("slice", slice.toString());
        slice.seek(0);
        assertEquals(0, slice.getFilePointer());
    }

    public void testCheckCacheHit() throws Exception {
        final OnDemandPrefetchBlockSnapshotIndexInput input = createIndexInput(23);
        // Block files exist on disk from initBlockFiles, but not in fileCache
        // checkCacheHit checks fileCache, not disk
        assertFalse(input.checkCacheHit(0));
    }

    public void testFetchNextNBlocksAtEnd() throws Exception {
        final OnDemandPrefetchBlockSnapshotIndexInput input = createIndexInput(23);
        int totalBlocks = input.getTotalBlocks();
        // fetchNextNBlocks at last block should not throw
        input.fetchNextNBlocks(totalBlocks - 1);
    }

    public void testDownloadBlocksAsync() throws Exception {
        final OnDemandPrefetchBlockSnapshotIndexInput input = createIndexInput(23);
        input.downloadBlocksAsync(0, 1, false);
        verify(transferManager, times(2)).fetchBlobAsync(any(BlobFetchRequest.class));
    }

    public void testChunkedRepositoryWithBlockSizeGreaterThanChunkSize() throws IOException {
        verifyChunkedRepository(8192, 2048, 15360);
    }

    public void testChunkedRepositoryWithBlockSizeLessThanChunkSize() throws IOException {
        verifyChunkedRepository(1024, 2048, 3072);
    }

    public void testChunkedRepositoryWithBlockSizeEqualToChunkSize() throws IOException {
        verifyChunkedRepository(2048, 2048, 15360);
    }

    private void verifyChunkedRepository(long blockSize, long repositoryChunkSize, long fileSize) throws IOException {
        when(transferManager.fetchBlob(any())).thenReturn(new ByteArrayIndexInput("test", new byte[(int) blockSize]));
        try (
            FSDirectory directory = new MMapDirectory(path, lockFactory);
            IndexInput indexInput = new OnDemandPrefetchBlockSnapshotIndexInput(
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
                    new org.opensearch.core.common.unit.ByteSizeValue(repositoryChunkSize)
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

    private void runAllTestsFor(int blockSizeShift) throws Exception {
        final OnDemandPrefetchBlockSnapshotIndexInput input = createIndexInput(blockSizeShift);
        final int blockSize = 1 << blockSizeShift;

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

    private OnDemandPrefetchBlockSnapshotIndexInput createIndexInput(int blockSizeShift) throws IOException {
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

        FSDirectory directory;
        try {
            directory = new MMapDirectory(path, lockFactory);
        } catch (IOException e) {
            fail("fail to create MMapDirectory: " + e.getMessage());
            return null;
        }

        initBlockFiles(blockSize, directory);

        return new OnDemandPrefetchBlockSnapshotIndexInput(
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
}
