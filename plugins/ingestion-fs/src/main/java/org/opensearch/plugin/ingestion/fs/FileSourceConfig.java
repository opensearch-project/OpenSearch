/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.fs;

import java.util.Map;

/**
 * Configuration holder for file-based ingestion source.
 */
public class FileSourceConfig {
    private static final String STREAM = "stream";
    private static final String BASE_DIR = "base_directory";

    private final String stream;
    private final String baseDirectory;

    /**
     * Initialize a new file-based source config.
     * @param params parameters for file-based indexing
     */
    public FileSourceConfig(Map<String, Object> params) {
        this.stream = (String) params.get(STREAM);
        this.baseDirectory = (String) params.get(BASE_DIR);
    }

    /**
     * Returns the stream name.
     */
    public String getStream() {
        return stream;
    }

    /**
     * Returns the base directory in which stream and shard specific files will be present.
     */
    public String getBaseDirectory() {
        return baseDirectory;
    }
}
