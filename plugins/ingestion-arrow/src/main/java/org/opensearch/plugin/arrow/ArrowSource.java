/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.arrow;

import java.io.Closeable;
import java.io.IOException;
import org.apache.arrow.vector.ipc.ArrowReader;

/**
 * Abstracts an Arrow-compatible data source that {@link ArrowShardConsumer} reads from.
 *
 * <p>Implementations are provided by an {@link ArrowSourceFactory} registered through the {@link
 * ArrowPlugin} extension point (see {@link ArrowPlugin#loadExtensions}), so this plugin never
 * depends on any concrete data-access technology (Lance, Iceberg, Delta Lake, etc.) — those
 * concerns, including credential management and storage-specific configuration, live entirely in
 * the extending plugin.
 */
public interface ArrowSource extends Closeable {

    /**
     * Returns the max row count in this dataset.
     *
     * @return max row count in this dataset
     */
    long getRowCount();

    /**
     * Get an {@link ArrowReader} from start (inclusive) row to end (exclusive) row
     *
     * @param start inclusive start row number
     * @param end exclusive end row number
     */
    ArrowReader getReader(long start, long end) throws IOException;

    /**
     * Similar to {@link #getReader(long, long)} with start is 0 and end is {@link #getRowCount()},
     * essentially get a reader for the whole dataset
     */
    default ArrowReader getReader() throws IOException {
        return getReader(0, getRowCount());
    }

    /**
     * Similar to {@link #getReader(long, long)} with end implicitly set to {@link #getRowCount()}
     *
     * @param start inclusive start row number
     */
    default ArrowReader getReader(long start) throws IOException {
        return getReader(start, getRowCount());
    }

    /**
     * Returns the version timestamp.
     *
     * @return timestamp in seconds of the version that is returned by {@link #getReader()}, or null
     *     if not available
     */
    Long getVersionTimestamp();

    /**
     * Returns the version number of the dataset.
     *
     * @return version number of the dataset, in String
     */
    String getVersion();
}
