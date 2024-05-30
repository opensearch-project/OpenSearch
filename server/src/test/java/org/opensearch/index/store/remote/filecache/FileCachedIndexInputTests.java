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
import org.apache.lucene.store.IndexOutput;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.file.Path;

public class FileCachedIndexInputTests extends OpenSearchTestCase {

    private FileCache fileCache;
    private Path filePath;
    private IndexInput underlyingIndexInput;
    private FileCachedIndexInput fileCachedIndexInput;

    private static final int FILE_CACHE_CAPACITY = 1000;
    private static final String TEST_FILE = "test_file";
    private static final String SLICE_DESC = "slice_description";

    @Before
    public void setup() throws IOException {
        Path basePath = createTempDir("FileCachedIndexInputTests");
        FSDirectory fsDirectory = FSDirectory.open(basePath);
        IndexOutput indexOutput = fsDirectory.createOutput(TEST_FILE, IOContext.DEFAULT);
        // Writing to the file so that it's size is not zero
        indexOutput.writeInt(100);
        indexOutput.close();
        filePath = basePath.resolve(TEST_FILE);
        underlyingIndexInput = fsDirectory.openInput(TEST_FILE, IOContext.DEFAULT);
        fileCache = FileCacheFactory.createConcurrentLRUFileCache(FILE_CACHE_CAPACITY, new NoopCircuitBreaker(CircuitBreaker.REQUEST));
        fileCachedIndexInput = new FileCachedIndexInput(fileCache, filePath, underlyingIndexInput);
        fileCache.put(filePath, new FullFileCachedIndexInput(fileCache, filePath, fileCachedIndexInput));
    }

    public void testClone() throws IOException {

        // Since the file ia already in cache and has refCount 1, activeUsage and totalUsage will be same
        assertTrue(isActiveAndTotalUsageSame());

        // Decrementing the refCount explicitly on the file which will make it inactive (as refCount will drop to 0)
        fileCache.decRef(filePath);
        assertFalse(isActiveAndTotalUsageSame());

        // After cloning the refCount will increase again and activeUsage and totalUsage will be same again
        FileCachedIndexInput clonedFileCachedIndexInput = fileCachedIndexInput.clone();
        assertTrue(isActiveAndTotalUsageSame());

        // Closing the clone will again decrease the refCount making it 0
        clonedFileCachedIndexInput.close();
        assertFalse(isActiveAndTotalUsageSame());
    }

    public void testSlice() throws IOException {

        // Throw IllegalArgumentException if offset is negative
        assertThrows(IllegalArgumentException.class, () -> fileCachedIndexInput.slice(SLICE_DESC, -1, 10));

        // Throw IllegalArgumentException if length is negative
        assertThrows(IllegalArgumentException.class, () -> fileCachedIndexInput.slice(SLICE_DESC, 5, -1));

        // Decrementing the refCount explicitly on the file which will make it inactive (as refCount will drop to 0)
        fileCache.decRef(filePath);
        assertFalse(isActiveAndTotalUsageSame());

        // Creating a slice will increase the refCount
        IndexInput slicedFileCachedIndexInput = fileCachedIndexInput.slice(SLICE_DESC, 1, 2);
        assertTrue(isActiveAndTotalUsageSame());

        // Closing the clone will again decrease the refCount making it 0
        slicedFileCachedIndexInput.close();
        assertFalse(isActiveAndTotalUsageSame());
    }

    private boolean isActiveAndTotalUsageSame() {
        return fileCache.usage().activeUsage() == fileCache.usage().usage();
    }
}
