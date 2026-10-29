/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.filecache;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.index.store.remote.file.BulkReadCountingIndexInput;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;

@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public class FileCachedIndexInputTests extends OpenSearchTestCase {

    protected FileCache fileCache;
    protected Path filePath;
    protected IndexInput underlyingIndexInput;
    private FileCachedIndexInput fileCachedIndexInput;

    protected static final int FILE_CACHE_CAPACITY = 1000;
    protected static final String TEST_FILE = "test_file";
    protected static final String SLICE_DESC = "slice_description";

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
        fileCache = FileCacheFactory.createConcurrentLRUFileCache(FILE_CACHE_CAPACITY);
    }

    protected void setupIndexInputAndAddToFileCache() {
        fileCachedIndexInput = new FileCachedIndexInput(fileCache, filePath, underlyingIndexInput);
        fileCache.put(filePath, new CachedFullFileIndexInput(fileCache, filePath, fileCachedIndexInput));
    }

    public void testClone() throws IOException {
        setupIndexInputAndAddToFileCache();

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
        setupIndexInputAndAddToFileCache();
        assertThrows(UnsupportedOperationException.class, () -> fileCachedIndexInput.slice(SLICE_DESC, 10, 100));
    }

    protected boolean isActiveAndTotalUsageSame() {
        return fileCache.activeUsage() == fileCache.usage();
    }

    public void testBulkReadsAreForwardedToUnderlyingInput() throws IOException {
        byte[] data = new byte[4096];
        random().nextBytes(data);
        ByteBuffer expected = ByteBuffer.wrap(data).order(ByteOrder.LITTLE_ENDIAN);
        BulkReadCountingIndexInput counting = new BulkReadCountingIndexInput("counting", data, 0, data.length);
        try (FileCachedIndexInput input = new FileCachedIndexInput(fileCache, filePath, counting)) {
            float[] floats = new float[128];
            input.seek(16);
            input.readFloats(floats, 0, floats.length);
            for (int i = 0; i < floats.length; i++) {
                assertEquals(expected.getFloat(16 + i * Float.BYTES), floats[i], 0f);
            }

            int[] ints = new int[128];
            input.seek(600);
            input.readInts(ints, 0, ints.length);
            for (int i = 0; i < ints.length; i++) {
                assertEquals(expected.getInt(600 + i * Integer.BYTES), ints[i]);
            }

            long[] longs = new long[64];
            input.seek(1200);
            input.readLongs(longs, 0, longs.length);
            for (int i = 0; i < longs.length; i++) {
                assertEquals(expected.getLong(1200 + i * Long.BYTES), longs[i]);
            }

            // each bulk read reached the underlying input as exactly one bulk call, not as element-wise reads
            assertEquals(1, counting.readFloatsCalls);
            assertEquals(1, counting.readIntsCalls);
            assertEquals(1, counting.readLongsCalls);
            assertEquals(0, counting.readIntCalls);
            assertEquals(0, counting.readLongCalls);
            assertEquals(0, counting.readByteCalls);
        }
    }
}
