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
    private static final String TOPIC = "topic";
    private static final String BASE_DIR = "base_directory";

    private final String topic;
    private final String baseDirectory;

    /**
     * Initialize a new file-based source config.
     * @param params parameters for file-based indexing
     */
    public FileSourceConfig(Map<String, Object> params) {
        this.topic = (String) params.get(TOPIC);
        this.baseDirectory = (String) params.get(BASE_DIR);
    }

    /**
     * Returns the topic name.
     */
    public String getTopic() {
        return topic;
    }

    /**
     * Returns the base directory in which topic and shard specific files will be present.
     */
    public String getBaseDirectory() {
        return baseDirectory;
    }
}
