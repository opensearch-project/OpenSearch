/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.search.ReferenceManager;
import org.opensearch.common.annotation.ExperimentalApi;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Lucene implementation of {@link EngineReaderManager}.
 * <p>
 * Wraps Lucene's {@link ReferenceManager} for {@link DirectoryReader}.
 * Acquire increments the ref count on the current reader;
 * release decrements it — same pattern as {@code DatafusionReaderManager}.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneReaderManager implements EngineReaderManager<DirectoryReader> {
    private final AtomicReference<DirectoryReader> currentReader = new AtomicReference<>();

    private final ReferenceManager<DirectoryReader> referenceManager;

    @SuppressWarnings("unchecked")
    public LuceneReaderManager(ReferenceManager<? extends DirectoryReader> referenceManager) {
        // Safe cast: ReferenceManager<OpenSearchDirectoryReader> is-a ReferenceManager<DirectoryReader>
        // for acquire/release purposes since we only use DirectoryReader as the return type.
        this.referenceManager = (ReferenceManager<DirectoryReader>) referenceManager;
    }

    @Override
    public DirectoryReader acquire() throws IOException {
        return referenceManager.acquire();
    }

    @Override
    public void release(DirectoryReader reader) throws IOException {
        referenceManager.release(reader);
    }

    /** Called after shard refresh with new catalog snapshot data. */
    public void afterRefresh(boolean didRefresh) throws IOException {
        if (!didRefresh) return;
        // TODO: acquire new catalog snapshot, create new directory reader,
        // swap atomically, decRef old reader
    }

    /** Called when files are deleted after merges. */
    public void onFileDeleted(Collection<String> files) throws IOException {
        // TODO: evict deleted files from cache manager
    }

    /** Set the initial reader during shard initialization. */
    public void setInitialReader(DirectoryReader reader) {
        currentReader.set(reader);
    }
}
