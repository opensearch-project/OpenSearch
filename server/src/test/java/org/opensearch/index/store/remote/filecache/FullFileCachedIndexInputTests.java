/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.filecache;

import org.apache.lucene.store.AlreadyClosedException;
import org.apache.lucene.store.IndexInput;

import java.io.IOException;

public class FullFileCachedIndexInputTests extends FileCachedIndexInputTests {
    private FullFileCachedIndexInput fullFileCachedIndexInput;

    @Override
    protected void setupIndexInputAndAddToFileCache() {
        fullFileCachedIndexInput = new FullFileCachedIndexInput(fileCache, filePath, underlyingIndexInput);
        fileCache.put(filePath, new CachedFullFileIndexInput(fileCache, filePath, fullFileCachedIndexInput));
    }

    @Override
    public void testClone() throws IOException {
        setupIndexInputAndAddToFileCache();

        // Since the file is already in cache and has refCount 1, activeUsage and totalUsage will be same
        assertTrue(isActiveAndTotalUsageSame());

        // Getting the file cache entry (which wil increase the ref count, hence doing dec ref immediately afterwards)
        CachedIndexInput cachedIndexInput = fileCache.get(filePath);
        fileCache.decRef(filePath);

        // Decrementing the refCount explicitly on the file which will make it inactive (as refCount will drop to 0)
        fileCache.decRef(filePath);
        assertFalse(isActiveAndTotalUsageSame());

        // After cloning the refCount will increase again and activeUsage and totalUsage will be same again
        FileCachedIndexInput clonedFileCachedIndexInput1 = fullFileCachedIndexInput.clone();
        FileCachedIndexInput clonedFileCachedIndexInput2 = clonedFileCachedIndexInput1.clone();
        FileCachedIndexInput clonedFileCachedIndexInput3 = clonedFileCachedIndexInput2.clone();
        assertTrue(isActiveAndTotalUsageSame());

        // closing the first level clone will close all subsequent level clones and reduce ref count to 0
        clonedFileCachedIndexInput1.close();
        assertFalse(isActiveAndTotalUsageSame());

        fileCache.prune();

        // since the file cache entry was evicted the corresponding CachedIndexInput will be closed and will throw exception when trying to
        // read the index input
        assertThrows(AlreadyClosedException.class, cachedIndexInput::getIndexInput);
    }

    @Override
    public void testSlice() throws IOException {
        setupIndexInputAndAddToFileCache();

        // Throw IllegalArgumentException if offset is negative
        assertThrows(IllegalArgumentException.class, () -> fullFileCachedIndexInput.slice(SLICE_DESC, -1, 10));

        // Throw IllegalArgumentException if length is negative
        assertThrows(IllegalArgumentException.class, () -> fullFileCachedIndexInput.slice(SLICE_DESC, 5, -1));

        // Decrementing the refCount explicitly on the file which will make it inactive (as refCount will drop to 0)
        fileCache.decRef(filePath);
        assertFalse(isActiveAndTotalUsageSame());

        // Creating a slice will increase the refCount
        IndexInput slicedFileCachedIndexInput = fullFileCachedIndexInput.slice(SLICE_DESC, 1, 2);
        assertTrue(isActiveAndTotalUsageSame());

        // Closing the parent will close all the slices as well decreasing the refCount to 0
        fullFileCachedIndexInput.close();
        assertFalse(isActiveAndTotalUsageSame());
    }
}
