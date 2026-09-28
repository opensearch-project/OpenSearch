/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.common;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.UUIDs;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.index.store.remote.utils.TransferManager;
import org.opensearch.node.Node;
import org.opensearch.storage.indexinput.BlockFetchRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for BlockTransferManager functionality.
 * Tests cover single block downloads, failure scenarios, duplicate handling, and concurrent operations.
 */
@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public class BlockTransferManagerTests extends OpenSearchTestCase {

    // Node and index configuration constants
    private static final String TEST_NODE_NAME = "test-node";
    private static final String TEST_INDEX_NAME = "test-index";
    private static final int TEST_NODE_PROCESSORS = 2;
    private static final int TEST_SHARD_COUNT = 1;
    private static final int TEST_REPLICA_COUNT = 0;

    // Test file constants
    private static final String TEST_FILE_NAME = "file1";
    private static final String TEST_BLOCK_FILE_NAME = "file1_block_0";

    // Test data constants
    private static final String HELLO_WORLD_CONTENT = "hello world";
    private static final String TEST_DATA_CONTENT = "test-data";
    private static final String SIMPLE_TEST_CONTENT = "abc";

    // Error message constants
    private static final String REMOTE_READ_ERROR_MESSAGE = "Failed to read remote block";

    // Test timing constants
    private static final long CONCURRENT_TEST_SLEEP_MS = 200L;

    // Block position constants
    private static final long BLOCK_START_POSITION = 0L;

    // Test instance variables
    private IndexSettings indexSettings;
    private FSDirectory fsDirectory;
    private BlockTransferManager blockTransferManager;
    private TestThreadPool testThreadPool;

    @Before
    public void setup() throws IOException {
        fsDirectory = FSDirectory.open(createTempDir());

        Settings settings = Settings.builder()
            .put(Node.NODE_NAME_SETTING.getKey(), TEST_NODE_NAME)
            .put(OpenSearchExecutors.NODE_PROCESSORS_SETTING.getKey(), TEST_NODE_PROCESSORS)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, TEST_SHARD_COUNT)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, TEST_REPLICA_COUNT)
            .put(IndexMetadata.SETTING_INDEX_UUID, UUIDs.randomBase64UUID())
            .build();

        IndexMetadata indexMetadata = IndexMetadata.builder(TEST_INDEX_NAME).settings(settings).build();

        indexSettings = new IndexSettings(indexMetadata, settings);
        testThreadPool = new TestThreadPool();
    }

    @After
    public void cleanup() throws IOException {
        Arrays.stream(fsDirectory.listAll()).forEach(fileName -> {
            try {
                fsDirectory.deleteFile(fileName);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        fsDirectory.close();
        if (testThreadPool != null) {
            testThreadPool.shutdown();
        }
    }

    /**
     * Test successful download of a single block.
     */
    public void testSingleBlockDownloadSuccess() throws Exception {
        byte[] content = HELLO_WORLD_CONTENT.getBytes(StandardCharsets.UTF_8);
        BlockFetchRequest request = createBlockFetchRequest(
            fsDirectory,
            TEST_FILE_NAME,
            TEST_BLOCK_FILE_NAME,
            BLOCK_START_POSITION,
            content.length
        );

        TransferManager.StreamReader reader = (name, pos, len) -> {
            if (name.equals(TEST_FILE_NAME) && pos == BLOCK_START_POSITION && len == content.length) {
                return new ByteArrayInputStream(content);
            }
            return null;
        };

        BlockTransferManager manager = new BlockTransferManager(reader, indexSettings, () -> testThreadPool);
        manager.fetchBlocksAsync(List.of(request));

        byte[] actualContent = new byte[content.length];
        fsDirectory.openInput(TEST_BLOCK_FILE_NAME, IOContext.READONCE).readBytes(actualContent, 0, content.length);
        Assert.assertArrayEquals(actualContent, content);
    }

    /**
     * Test handling of block download failures.
     */
    public void testBlockDownloadFailure() {
        byte[] content = HELLO_WORLD_CONTENT.getBytes(StandardCharsets.UTF_8);
        BlockFetchRequest request = createBlockFetchRequest(
            fsDirectory,
            TEST_FILE_NAME,
            TEST_BLOCK_FILE_NAME,
            BLOCK_START_POSITION,
            content.length
        );

        TransferManager.StreamReader reader = (name, pos, len) -> { throw new IOException(REMOTE_READ_ERROR_MESSAGE); };

        BlockTransferManager manager = new BlockTransferManager(reader, indexSettings, () -> testThreadPool);
        assertThrows(IOException.class, () -> manager.fetchBlocksAsync(List.of(request)));
    }

    /**
     * Test that duplicate downloads are properly skipped.
     */
    public void testDuplicateDownloadIsSkipped() throws Exception {
        byte[] data = TEST_DATA_CONTENT.getBytes(StandardCharsets.UTF_8);
        BlockFetchRequest request = createBlockFetchRequest(
            fsDirectory,
            TEST_FILE_NAME,
            TEST_BLOCK_FILE_NAME,
            BLOCK_START_POSITION,
            data.length
        );

        TransferManager.StreamReader reader = mock(TransferManager.StreamReader.class);
        when(reader.read(any(), anyLong(), anyLong())).thenReturn(new ByteArrayInputStream(data));

        BlockTransferManager manager = new BlockTransferManager(reader, indexSettings, () -> testThreadPool);

        // First fetch triggers the download
        manager.fetchBlocksAsync(List.of(request));
        // Second fetch should skip download since file exists
        manager.fetchBlocksAsync(List.of(request));

        verify(reader, times(1)).read(any(), anyLong(), anyLong());
    }

    /**
     * Test that concurrent fetches of the same file result in only one download.
     */
    public void testConcurrentFetchSameFileOnlyOneDownload() throws Exception {
        byte[] data = SIMPLE_TEST_CONTENT.getBytes(StandardCharsets.UTF_8);
        BlockFetchRequest request = createBlockFetchRequest(
            fsDirectory,
            TEST_FILE_NAME,
            TEST_BLOCK_FILE_NAME,
            BLOCK_START_POSITION,
            data.length
        );

        CountDownLatch latch = new CountDownLatch(1);
        TransferManager.StreamReader reader = (name, pos, len) -> {
            try {
                latch.await(); // hold until signaled
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return new ByteArrayInputStream(data);
        };

        BlockTransferManager manager = new BlockTransferManager(reader, indexSettings, () -> testThreadPool);

        Thread firstThread = new Thread(() -> {
            try {
                manager.fetchBlocksAsync(List.of(request));
            } catch (Exception ignored) {
                // Exception handling for test thread
            }
        });

        Thread secondThread = new Thread(() -> {
            try {
                manager.fetchBlocksAsync(List.of(request));
            } catch (Exception ignored) {
                // Exception handling for test thread
            }
        });

        firstThread.start();
        secondThread.start();

        Thread.sleep(CONCURRENT_TEST_SLEEP_MS); // let both threads start and block on latch
        latch.countDown(); // let read proceed

        firstThread.join();
        secondThread.join();

        assertTrue(Arrays.stream(fsDirectory.listAll()).anyMatch(fileName -> TEST_BLOCK_FILE_NAME.equals(fileName)));
    }

    // Additional test cases for new functionality

    public void testDownloadBlobWithExistingTempFile() throws Exception {
        byte[] data = TEST_DATA_CONTENT.getBytes(StandardCharsets.UTF_8);
        BlockFetchRequest request = createBlockFetchRequest(
            fsDirectory,
            TEST_FILE_NAME,
            TEST_BLOCK_FILE_NAME,
            BLOCK_START_POSITION,
            data.length
        );

        // Create a temp file first
        Path tempFile = fsDirectory.getDirectory().resolve(TEST_BLOCK_FILE_NAME + ".part");
        Files.write(tempFile, "old data".getBytes(StandardCharsets.UTF_8));
        assertTrue(Files.exists(tempFile));

        TransferManager.StreamReader reader = (name, pos, len) -> new ByteArrayInputStream(data);
        BlockTransferManager manager = new BlockTransferManager(reader, indexSettings, () -> testThreadPool);

        manager.fetchBlocksAsync(List.of(request));

        // Verify temp file was cleaned up and final file exists
        assertFalse(Files.exists(tempFile));
        assertTrue(Arrays.stream(fsDirectory.listAll()).anyMatch(fileName -> TEST_BLOCK_FILE_NAME.equals(fileName)));
    }

    public void testDownloadBlobWithExistingFinalFile() throws Exception {
        byte[] data = TEST_DATA_CONTENT.getBytes(StandardCharsets.UTF_8);
        BlockFetchRequest request = createBlockFetchRequest(
            fsDirectory,
            TEST_FILE_NAME,
            TEST_BLOCK_FILE_NAME,
            BLOCK_START_POSITION,
            data.length
        );

        // Create the final file first
        Path finalFile = fsDirectory.getDirectory().resolve(TEST_BLOCK_FILE_NAME);
        Files.write(finalFile, "existing data".getBytes(StandardCharsets.UTF_8));
        assertTrue(Files.exists(finalFile));

        TransferManager.StreamReader reader = mock(TransferManager.StreamReader.class);
        BlockTransferManager manager = new BlockTransferManager(reader, indexSettings, () -> testThreadPool);

        manager.fetchBlocksAsync(List.of(request));

        // Verify reader was never called since file already exists
        verify(reader, never()).read(any(), anyLong(), anyLong());
    }

    public void testDownloadBlobWithIOExceptionDuringCopy() throws Exception {
        byte[] data = TEST_DATA_CONTENT.getBytes(StandardCharsets.UTF_8);
        BlockFetchRequest request = createBlockFetchRequest(
            fsDirectory,
            TEST_FILE_NAME,
            TEST_BLOCK_FILE_NAME,
            BLOCK_START_POSITION,
            data.length
        );

        TransferManager.StreamReader reader = (name, pos, len) -> { throw new IOException("Stream read failed"); };

        BlockTransferManager manager = new BlockTransferManager(reader, indexSettings, () -> testThreadPool);
        assertThrows(IOException.class, () -> manager.fetchBlocksAsync(List.of(request)));
    }

    public void testMultipleBlockFetchRequests() throws Exception {
        byte[] data1 = "block1".getBytes(StandardCharsets.UTF_8);
        byte[] data2 = "block2".getBytes(StandardCharsets.UTF_8);

        BlockFetchRequest request1 = createBlockFetchRequest(fsDirectory, "file1", "file1_block_0", 0L, data1.length);
        BlockFetchRequest request2 = createBlockFetchRequest(fsDirectory, "file2", "file2_block_0", 0L, data2.length);

        TransferManager.StreamReader reader = (name, pos, len) -> {
            if ("file1".equals(name)) {
                return new ByteArrayInputStream(data1);
            } else if ("file2".equals(name)) {
                return new ByteArrayInputStream(data2);
            }
            throw new IllegalArgumentException("Unknown file: " + name);
        };

        BlockTransferManager manager = new BlockTransferManager(reader, indexSettings, () -> testThreadPool);
        manager.fetchBlocksAsync(List.of(request1, request2));

        // Verify both files were created
        String[] files = fsDirectory.listAll();
        assertTrue(Arrays.asList(files).contains("file1_block_0"));
        assertTrue(Arrays.asList(files).contains("file2_block_0"));
    }

    public void testExecutionExceptionHandling() throws Exception {
        byte[] data = TEST_DATA_CONTENT.getBytes(StandardCharsets.UTF_8);
        BlockFetchRequest request = createBlockFetchRequest(
            fsDirectory,
            TEST_FILE_NAME,
            TEST_BLOCK_FILE_NAME,
            BLOCK_START_POSITION,
            data.length
        );

        TransferManager.StreamReader reader = (name, pos, len) -> { throw new IOException("Wrapped IOException"); };

        BlockTransferManager manager = new BlockTransferManager(reader, indexSettings, () -> testThreadPool);

        IOException thrown = assertThrows(IOException.class, () -> { manager.fetchBlocksAsync(List.of(request)); });

        assertEquals("Wrapped IOException", thrown.getCause().getCause().getMessage());
    }

    /**
     * Creates a BlockFetchRequest with the specified parameters.
     *
     * @param directory the FSDirectory to use
     * @param fileName the name of the file
     * @param blockFileName the name of the block file
     * @param blockStart the starting position of the block
     * @param blockSize the size of the block
     * @return a configured BlockFetchRequest
     */
    private BlockFetchRequest createBlockFetchRequest(
        FSDirectory directory,
        String fileName,
        String blockFileName,
        long blockStart,
        long blockSize
    ) {

        return BlockFetchRequest.builder()
            .directory(directory)
            .fileName(fileName)
            .blockFileName(blockFileName)
            .blockStart(blockStart)
            .blockSize(blockSize)
            .build();
    }

    /**
     * Test ThreadPool implementation with 4 threads for testing BlockTransferManager.
     */
    private static class TestThreadPool extends ThreadPool {
        private final ExecutorService executorService;

        public TestThreadPool() {
            super(Settings.builder().put("node.name", "test-node").build());
            this.executorService = Executors.newFixedThreadPool(4);
        }

        @Override
        public ExecutorService executor(String name) {
            return executorService;
        }

        @Override
        public void shutdown() {
            executorService.shutdown();
            super.shutdown();
        }
    }
}
