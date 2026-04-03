/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.shard.ShardPath;

import java.io.IOException;
import java.util.List;

/**
 * Unified SPI for back-end storage and query engines.
 * <p>
 * Each implementation provides reader lifecycle management (via
 * {@link #createReaderManager}) and declares supported data formats.
 * The type parameter {@code R} carries the reader type, eliminating
 * unsafe casts at the boundary.
 * <p>
 * Plugins that also support the analytics query path should additionally
 * implement {@code SearchExecEngineProvider} from the analytics framework.
 *
 * @param <R> the reader type produced by this backend's reader manager
 * @opensearch.internal
 */
public interface SearchBackEndPlugin<R> {

    /** Unique backend name (e.g., "datafusion", "lucene"). */
    String name();

    /** Returns the data formats this backend can read and query. */
    List<DataFormat> getSupportedFormats();

    /**
     * Creates a reader manager for the given data format and shard.
     * The {@link Committer} provides access to the backing store (e.g., IndexWriter)
     * so that the reader manager can open readers from the same writer.
     *
     * @param committer the committer holding the backing store, or null if not available
     * @param format the data format
     * @param shardPath the shard path
     * @return the reader manager
     * @throws IOException if reader creation fails
     */
    EngineReaderManager<?> createReaderManager(Committer committer, DataFormat format, ShardPath shardPath) throws IOException;
}
