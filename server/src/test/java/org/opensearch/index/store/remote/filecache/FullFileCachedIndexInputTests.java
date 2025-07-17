/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.filecache;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.apache.lucene.store.AlreadyClosedException;
import org.opensearch.index.store.remote.file.CleanerDaemonThreadLeakFilter;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

@ThreadLeakFilters(filters = CleanerDaemonThreadLeakFilter.class)
public class FullFileCachedIndexInputTests extends FileCachedIndexInputTests {
    private FullFileCachedIndexInput fullFileCachedIndexInput;

    @Override
    protected void setupIndexInputAndAddToFileCache() {
        fullFileCachedIndexInput = new FullFileCachedIndexInput(fileCache, filePath, underlyingIndexInput);
        // Putting in the file cache would increase refCount to 1
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

        // Since no clones have been done, refCount should be zero
        assertEquals((int) fileCache.getRef(filePath), 0);

        createUnclosedClonesSlices(false);
        triggerGarbageCollectionAndAssertClonesClosed();

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

        // Since no clones have been done, refCount should be zero
        assertEquals((int) fileCache.getRef(filePath), 0);

        createUnclosedClonesSlices(true);
        triggerGarbageCollectionAndAssertClonesClosed();

        assertFalse(isActiveAndTotalUsageSame());
    }

    private void triggerGarbageCollectionAndAssertClonesClosed() {
        try {
            // Clones/Slices will be phantom reachable now, triggering gc should call close on them
            assertBusy(() -> {
                System.gc(); // Do not rely on GC to be deterministic, hence the polling
                assertEquals(
                    "Expected refCount to drop to zero as all clones/slices should have closed",
                    (int) fileCache.getRef(filePath),
                    0
                );
            }, 5, TimeUnit.SECONDS);
        } catch (Exception e) {
            logger.error("Exception thrown while triggering gc", e);
            fail();
        }
    }

    private void createUnclosedClonesSlices(boolean createSlice) throws IOException {
        int NUM_OF_CLONES = 3;
        for (int i = 0; i < NUM_OF_CLONES; i++) {
            if (createSlice) fullFileCachedIndexInput.slice("slice", 1, 2);
            else fullFileCachedIndexInput.clone();
        }
        assertEquals((int) fileCache.getRef(filePath), NUM_OF_CLONES);
    }
}
