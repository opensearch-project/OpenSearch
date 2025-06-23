/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.filecache;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.breaker.TestCircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.CircuitBreakingException;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.index.store.remote.directory.RemoteSnapshotDirectoryFactory;
import org.opensearch.index.store.remote.utils.FileTypeUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.List;

public class FileCacheTests extends OpenSearchTestCase {
    // need concurrency level to be static to make these tests more deterministic because capacity per segment is dependent on
    // (total capacity) / (concurrency level) so having high concurrency level might trigger early evictions which is tolerable in real life
    // but fatal to these tests
    private final static int CONCURRENCY_LEVEL = 16;
    private final static int MEGA_BYTES = 1024 * 1024;
    private final static String FAKE_PATH_SUFFIX = "Suffix";
    private Path path;

    @Before
    public void init() throws Exception {
        path = createTempDir("FileCacheTests");
    }

    private FileCache createFileCache(long capacity) {
        return FileCacheFactory.createConcurrentLRUFileCache(capacity, CONCURRENCY_LEVEL, new NoopCircuitBreaker(CircuitBreaker.REQUEST));
    }

    private FileCache createCircuitBreakingFileCache(long capacity) {
        TestCircuitBreaker testCircuitBreaker = new TestCircuitBreaker();
        testCircuitBreaker.startBreaking();
        return FileCacheFactory.createConcurrentLRUFileCache(capacity, CONCURRENCY_LEVEL, testCircuitBreaker);
    }

    private Path createPath(String middle) {
        return path.resolve(middle).resolve(FAKE_PATH_SUFFIX);
    }

    @SuppressForbidden(reason = "creating a test file for cache")
    private void createFile(String indexName, String shardId, String fileName) throws IOException {
        Path folderPath = path.resolve(NodeEnvironment.CACHE_FOLDER)
            .resolve(indexName)
            .resolve(shardId)
            .resolve(RemoteSnapshotDirectoryFactory.LOCAL_STORE_LOCATION);
        Path filePath = folderPath.resolve(fileName);
        Files.createDirectories(folderPath);
        Files.createFile(filePath);
        Files.write(filePath, "test-data".getBytes());
    }

    @SuppressForbidden(reason = "creating a test file for cache")
    private void createWarmIndexFile(String indexName, String shardId, String fileName) throws IOException {
        Path folderPath = path.resolve(NodeEnvironment.INDICES_FOLDER)
            .resolve(indexName)
            .resolve(shardId)
            .resolve(FileTypeUtils.INDICES_FOLDER_IDENTIFIER);
        Path filePath = folderPath.resolve(fileName);
        Files.createDirectories(folderPath);
        Files.createFile(filePath);
        Files.write(filePath, "test-data".getBytes());
    }

    // test get method
    public void testGet() {
        FileCache fileCache = createFileCache(8 * MEGA_BYTES);
        for (int i = 0; i < 4; i++) {
            fileCache.put(createPath(Integer.toString(i)), new StubCachedIndexInput(8 * MEGA_BYTES));
        }
        // verify all blocks are put into file cache
        for (int i = 0; i < 4; i++) {
            assertNotNull(fileCache.get(createPath(Integer.toString(i))));
        }
    }

    public void testGetWithCachedFullFileIndexInput() throws IOException {
        FileCache fileCache = createFileCache(1 * 1000);
        for (int i = 0; i < 4; i++) {
            Path filePath = path.resolve(NodeEnvironment.CACHE_FOLDER)
                .resolve("indexName")
                .resolve("shardId")
                .resolve(Integer.toString(i))
                .resolve(RemoteSnapshotDirectoryFactory.LOCAL_STORE_LOCATION);
            createFile("indexName", "shardId/".concat(Integer.toString(i)), "test_file");
            FSDirectory fsDirectory = FSDirectory.open(filePath);
            FileCachedIndexInput fileCachedIndexInput = new FileCachedIndexInput(
                fileCache,
                filePath,
                fsDirectory.openInput("test_file", IOContext.DEFAULT)
            );
            fileCache.put(filePath.resolve("test_file"), new CachedFullFileIndexInput(fileCache, filePath, fileCachedIndexInput));
        }
        // verify all files are put into file cache
        for (int i = 0; i < 4; i++) {
            Path filePath = path.resolve(NodeEnvironment.CACHE_FOLDER)
                .resolve("indexName")
                .resolve("shardId")
                .resolve(Integer.toString(i))
                .resolve(RemoteSnapshotDirectoryFactory.LOCAL_STORE_LOCATION);
            assertNotNull(fileCache.get(filePath.resolve("test_file")));

            fileCache.decRef(filePath);
            fileCache.decRef(filePath);
        }

        // Test eviction by adding more files to exceed cache capacity
        for (int i = 4; i < 8000; i++) {
            Path filePath = path.resolve(NodeEnvironment.CACHE_FOLDER)
                .resolve("indexName")
                .resolve("shardId")
                .resolve(Integer.toString(i))
                .resolve(RemoteSnapshotDirectoryFactory.LOCAL_STORE_LOCATION);
            createFile("indexName", "shardId/".concat(Integer.toString(i)), "test_file");
            FSDirectory fsDirectory = FSDirectory.open(filePath);
            FileCachedIndexInput fileCachedIndexInput = new FileCachedIndexInput(
                fileCache,
                filePath,
                fsDirectory.openInput("test_file", IOContext.DEFAULT)
            );
            fileCache.put(filePath.resolve("test_file"), new CachedFullFileIndexInput(fileCache, filePath, fileCachedIndexInput));
        }

        // Verify some of the original files were evicted
        boolean someEvicted = false;
        for (int i = 0; i < 8000; i++) {
            Path filePath = path.resolve(NodeEnvironment.CACHE_FOLDER)
                .resolve("indexName")
                .resolve("shardId")
                .resolve(Integer.toString(i))
                .resolve(RemoteSnapshotDirectoryFactory.LOCAL_STORE_LOCATION);
            if (fileCache.get(filePath) == null) {
                someEvicted = true;
                break;
            }
        }
        assertTrue("Expected some files to be evicted", someEvicted);
    }

    public void testGetThrowException() {
        assertThrows(NullPointerException.class, () -> {
            FileCache fileCache = createFileCache(MEGA_BYTES);
            fileCache.get(null);
        });
    }

    public void testPutThrowException() {
        assertThrows(NullPointerException.class, () -> {
            FileCache fileCache = createFileCache(MEGA_BYTES);
            fileCache.put(null, null);
        });
    }

    public void testPutThrowCircuitBreakingException() {
        FileCache fileCache = createCircuitBreakingFileCache(MEGA_BYTES);
        Path path = createPath("0");
        assertThrows(CircuitBreakingException.class, () -> fileCache.put(path, new StubCachedIndexInput(8 * MEGA_BYTES)));
        assertNull(fileCache.get(path));
    }

    public void testCompute() {
        FileCache fileCache = createFileCache(MEGA_BYTES);
        Path path = createPath("0");
        fileCache.put(path, new StubCachedIndexInput(8 * MEGA_BYTES));
        fileCache.incRef(path);
        fileCache.compute(path, (p, i) -> null);
        // item will be removed
        assertEquals(fileCache.size(), 0);
    }

    public void testComputeThrowException() {
        assertThrows(NullPointerException.class, () -> {
            FileCache fileCache = createFileCache(MEGA_BYTES);
            fileCache.compute(null, null);
        });
    }

    public void testComputeThrowCircuitBreakingException() {
        FileCache fileCache = createCircuitBreakingFileCache(MEGA_BYTES);
        Path path = createPath("0");
        assertThrows(CircuitBreakingException.class, () -> fileCache.compute(path, (p, i) -> new StubCachedIndexInput(8 * MEGA_BYTES)));
        assertNull(fileCache.get(path));
    }

    public void testRemove() {
        FileCache fileCache = createFileCache(MEGA_BYTES);
        for (int i = 0; i < 4; i++) {
            fileCache.put(createPath(Integer.toString(i)), new StubCachedIndexInput(8 * MEGA_BYTES));
        }

        fileCache.remove(createPath("0"));
        fileCache.remove(createPath("0"));
        fileCache.remove(createPath("1"));
        assertEquals(fileCache.size(), 2);
        assertNotNull(fileCache.get(createPath("2")));
        assertNotNull(fileCache.get(createPath("3")));
    }

    public void testRemoveThrowException() {
        assertThrows(NullPointerException.class, () -> {
            FileCache fileCache = createFileCache(MEGA_BYTES);
            fileCache.remove(null);
        });
    }

    public void testIncDecRef() {
        FileCache fileCache = createFileCache(MEGA_BYTES);
        for (int i = 0; i < 4; i++) {
            fileCache.put(createPath(Integer.toString(i)), new StubCachedIndexInput(8 * MEGA_BYTES));
        }

        // try to evict previous IndexInput
        for (int i = 1000; i < 3000; i++) {
            putAndDecRef(fileCache, i, 8 * MEGA_BYTES);
        }

        // IndexInput with refcount greater than 0 will not be evicted
        for (int i = 0; i < 4; i++) {
            assertNotNull(fileCache.get(createPath(Integer.toString(i))));
            assertNotNull(fileCache.getRef(createPath(Integer.toString(i))));
            assertEquals(2, (int) fileCache.getRef(createPath(Integer.toString(i))));
            fileCache.decRef(createPath(Integer.toString(i)));
        }

        // decrease ref
        for (int i = 0; i < 4; i++) {
            fileCache.decRef(createPath(Integer.toString(i)));
            assertNotNull(fileCache.getRef(createPath(Integer.toString(i))));
            assertEquals(0, (int) fileCache.getRef(createPath(Integer.toString(i))));
        }

        // try to evict previous IndexInput again
        for (int i = 3000; i < 5000; i++) {
            putAndDecRef(fileCache, i, 8 * MEGA_BYTES);
        }

        for (int i = 0; i < 4; i++) {
            assertNull(fileCache.get(createPath(Integer.toString(i))));
            assertNull(fileCache.getRef(createPath(Integer.toString(i))));
        }
    }

    public void testIncRefThrowException() {
        assertThrows(NullPointerException.class, () -> {
            FileCache fileCache = createFileCache(MEGA_BYTES);
            fileCache.incRef(null);
        });

    }

    public void testDecRefThrowException() {
        assertThrows(NullPointerException.class, () -> {
            FileCache fileCache = createFileCache(MEGA_BYTES);
            fileCache.decRef(null);
        });

    }

    public void testCapacity() {
        FileCache fileCache = createFileCache(MEGA_BYTES);
        assertEquals(fileCache.capacity(), MEGA_BYTES);
    }

    public void testSize() {
        FileCache fileCache = createFileCache(MEGA_BYTES);
        for (int i = 0; i < 4; i++) {
            fileCache.put(createPath(Integer.toString(i)), new StubCachedIndexInput(8 * MEGA_BYTES));
        }
        // test file cache size
        assertEquals(fileCache.size(), 4);
    }

    public void testPrune() {
        FileCache fileCache = createFileCache(MEGA_BYTES);
        for (int i = 0; i < 4; i++) {
            putAndDecRef(fileCache, i, 8 * MEGA_BYTES);
        }
        // before prune
        assertTrue(fileCache.size() >= 1);

        fileCache.prune();
        // after prune
        assertEquals(0, fileCache.size());
    }

    public void testPruneWithPredicate() {
        FileCache fileCache = createFileCache(MEGA_BYTES);
        for (int i = 0; i < 4; i++) {
            putAndDecRef(fileCache, i, 8 * MEGA_BYTES);
        }

        // before prune
        assertTrue(fileCache.size() >= 1);

        // after prune with false predicate
        fileCache.prune(path -> false);
        assertTrue(fileCache.size() >= 1);

        // after prune with true predicate
        fileCache.prune(path -> true);
        assertEquals(0, fileCache.size());
    }

    public void testUsage() {
        FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(
            16 * MEGA_BYTES,
            1,
            new NoopCircuitBreaker(CircuitBreaker.REQUEST)
        );
        putAndDecRef(fileCache, 0, 16 * MEGA_BYTES);

        long expectedCacheUsage = 16 * MEGA_BYTES;
        long expectedActiveCacheUsage = 0;
        long realCacheUsage = fileCache.usage();
        long realActiveCacheUsage = fileCache.activeUsage();

        assertEquals(expectedCacheUsage, realCacheUsage);
        assertEquals(expectedActiveCacheUsage, realActiveCacheUsage);
    }

    public void testStats() {
        FileCache fileCache = createFileCache(MEGA_BYTES);
        for (int i = 0; i < 4; i++) {
            fileCache.put(createPath(Integer.toString(i)), new StubCachedIndexInput(8 * MEGA_BYTES));
        }
        // cache hits
        fileCache.get(createPath("0"));
        fileCache.get(createPath("1"));
        // cache misses
        fileCache.get(createPath("-1"));
        fileCache.get(createPath("-2"));

        assertEquals(fileCache.stats().hitCount(), 2);

        // do some eviction here
        for (int i = 0; i < 2000; i++) {
            putAndDecRef(fileCache, i, 8 * MEGA_BYTES);
        }
        assertTrue(fileCache.stats().evictionCount() > 0);
        assertTrue(fileCache.stats().evictionWeight() > 0);

    }

    public void testCacheRestore() throws IOException {
        String indexName = "test-index";
        String shardId = "0";
        createFile(indexName, shardId, "test.0");
        FileCache fileCache = createFileCache(MEGA_BYTES);
        assertEquals(0, fileCache.usage());
        Path fileCachePath = path.resolve(NodeEnvironment.CACHE_FOLDER).resolve(indexName).resolve(shardId);
        fileCache.restoreFromDirectory(List.of(fileCachePath));
        assertTrue(fileCache.usage() > 0);
        assertEquals(0, fileCache.activeUsage());
    }

    public void testCloseIndexInputReferences() throws IOException {
        FileCache fileCache = createFileCache(MEGA_BYTES);
        // Add some entries to cache
        int numEntries = 2;
        Path tempDir = createTempDir();
        Path path1 = tempDir.resolve("test1.tmp");
        Path path2 = tempDir.resolve("test2.tmp");

        // Create the files
        Files.createFile(path1);
        Files.createFile(path2);

        try {
            fileCache.put(path1, new StubCachedIndexInput(8 * MEGA_BYTES));
            fileCache.incRef(path1); // Increase reference count
            fileCache.put(path2, new StubCachedIndexInput(8 * MEGA_BYTES));
            fileCache.incRef(path2); // Increase reference count
            // Verify initial state
            assertEquals(numEntries, fileCache.size());
            // Close all references
            fileCache.closeIndexInputReferences();
            // Verify cache is empty
            assertEquals(0, fileCache.size());
            // Verify all entries are removed
            assertNull(fileCache.get(path1));
            assertNull(fileCache.get(path2));
            // Verify path still exists
            assertTrue(Files.exists(path1));
            assertTrue(Files.exists(path2));
        } finally {
            Files.deleteIfExists(path1);
            Files.deleteIfExists(path2);
        }
    }

    public void testRestoreFromEmptyDirectory() throws IOException {
        FileCache fileCache = createFileCache(MEGA_BYTES);
        Path emptyDir = createTempDir();
        fileCache.restoreFromDirectory(List.of(emptyDir));
        assertEquals(0, fileCache.usage());
        assertEquals(0, fileCache.activeUsage());
    }

    public void testRestoreWithNonExistentDirectory() {
        FileCache fileCache = createFileCache(MEGA_BYTES);
        Path nonExistentPath = path.resolve("non-existent");

        fileCache.restoreFromDirectory(List.of(nonExistentPath));
        assertEquals(0, fileCache.usage());
    }

    public void testWarmIndexCacheRestore() throws IOException {
        String indexName = "test-warm-index";
        String shardId = "0";
        createWarmIndexFile(indexName, shardId, "test.0");
        FileCache fileCache = createFileCache(MEGA_BYTES);
        assertEquals(0, fileCache.usage());
        Path indicesCachePath = path.resolve(NodeEnvironment.INDICES_FOLDER).resolve(indexName).resolve(shardId);
        fileCache.restoreFromDirectory(List.of(indicesCachePath));
        assertTrue(fileCache.usage() > 0);
        assertEquals(0, fileCache.activeUsage());
    }

    public void testRestoreFromMultipleDirectories() throws IOException {
        String index1 = "test-index-1";
        String index2 = "test-warm-index-2";
        String shardId = "0";

        // Create files in both cache and indices directories
        createFile(index1, shardId, "test1.0");
        createWarmIndexFile(index2, shardId, "test2.0");

        FileCache fileCache = createFileCache(MEGA_BYTES);
        Path cachePath = path.resolve(NodeEnvironment.CACHE_FOLDER).resolve(index1).resolve(shardId);
        Path indicesPath = path.resolve(NodeEnvironment.INDICES_FOLDER).resolve(index2).resolve(shardId);

        fileCache.restoreFromDirectory(List.of(cachePath, indicesPath));
        assertTrue(fileCache.usage() > 0);
        assertEquals(0, fileCache.activeUsage());
    }

    public void testRestoreWithInvalidWarmIndexFiles() throws IOException {
        String indexName = "test-warm-index";
        String shardId = "0";

        // Create a valid warm index file
        createWarmIndexFile(indexName, shardId, "valid.0");

        // Create an invalid/corrupt warm index file
        Path invalidFilePath = path.resolve(NodeEnvironment.INDICES_FOLDER)
            .resolve(indexName)
            .resolve(shardId)
            .resolve(FileTypeUtils.INDICES_FOLDER_IDENTIFIER)
            .resolve("invalid.0");
        Files.createDirectories(invalidFilePath.getParent());
        Files.createFile(invalidFilePath);
        // Make file unreadable
        Files.setPosixFilePermissions(invalidFilePath, PosixFilePermissions.fromString("---------"));

        FileCache fileCache = createFileCache(MEGA_BYTES);
        Path indicesPath = path.resolve(NodeEnvironment.INDICES_FOLDER).resolve(indexName).resolve(shardId);

        // Should handle the invalid file gracefully
        fileCache.restoreFromDirectory(List.of(indicesPath));
        assertTrue("File cache should contain at least the valid file", fileCache.usage() > 0);
        assertEquals("No files should be actively used", 0, fileCache.activeUsage());
    }

    private void putAndDecRef(FileCache cache, int path, long indexInputSize) {
        final Path key = createPath(Integer.toString(path));
        cache.put(key, new StubCachedIndexInput(indexInputSize));
        cache.decRef(key);
    }

    public static class StubCachedIndexInput implements CachedIndexInput {

        private final long length;

        public StubCachedIndexInput(long length) {
            this.length = length;
        }

        @Override
        public IndexInput getIndexInput() {
            return null;
        }

        @Override
        public long length() {
            return length;
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void close() throws Exception {

        }
    }
}
