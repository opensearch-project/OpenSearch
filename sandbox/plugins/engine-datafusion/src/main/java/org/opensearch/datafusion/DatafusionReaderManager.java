/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.EngineReaderManager;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Manages {@link DatafusionReader} instances (native memory).
 * <p>
 * Acquire returns a DatafusionReader with incremented ref count;
 * release decrements it. On refresh, a new reader is swapped in
 * atomically from the updated catalog snapshot.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DatafusionReaderManager implements EngineReaderManager<DatafusionReader> {

    private final AtomicReference<DatafusionReader> currentReader = new AtomicReference<>();

    @Override
    public DatafusionReader acquire() throws IOException {
        DatafusionReader reader = currentReader.get();
        if (reader == null) {
            throw new IOException("No DataFusion reader available");
        }
        reader.incRef();
        return reader;
    }

    @Override
    public void release(DatafusionReader reader) throws IOException {
        reader.decRef();
    }

    /** Called after shard refresh with new catalog snapshot data. */
    public void afterRefresh(boolean didRefresh) throws IOException {
        if (!didRefresh) return;
        // TODO: acquire new catalog snapshot, create new DatafusionReader,
        // swap atomically, decRef old reader
    }

    /** Called when files are deleted after merges. */
    public void onFileDeleted(Collection<String> files) throws IOException {
        // TODO: evict deleted files from cache manager
    }

    /** Set the initial reader during shard initialization. */
    public void setInitialReader(DatafusionReader reader) {
        currentReader.set(reader);
    }
}
