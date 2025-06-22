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

    public FileSourceConfig(Map<String, Object> params) {
        this.topic = (String) params.get(TOPIC);
        this.baseDirectory = (String) params.get(BASE_DIR);
    }

    public String getTopic() {
        return topic;
    }

    public String getBaseDirectory() {
        return baseDirectory;
    }
}
