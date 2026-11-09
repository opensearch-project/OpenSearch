/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.hive;

import java.io.Closeable;
import java.io.IOException;
import java.util.Map;

/**
 * Reads rows from a single Hive data file. Implementations handle format-specific
 * logic (Parquet, ORC, etc.) while HiveShardConsumer handles partition discovery
 * and Metastore interaction.
 */
public interface HiveFileReader extends Closeable {

    /**
     * Reads the next row from the file.
     *
     * @return a map of column names to values, or null if no more rows
     * @throws IOException if an I/O error occurs
     */
    Map<String, Object> readNext() throws IOException;
}
