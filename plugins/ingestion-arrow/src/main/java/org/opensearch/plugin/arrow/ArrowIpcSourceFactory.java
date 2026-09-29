/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.arrow;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import org.opensearch.core.util.ConfigurationUtils;

/**
 * Built-in {@link ArrowSourceFactory} for {@link ArrowIpcSource}: reads a local Arrow IPC (file
 * format) file, specified via {@link #ARROW_IPC_PATH}. Registered directly by {@link ArrowPlugin}
 * (not through the {@code extendedPlugins} SPI mechanism, since a plugin cannot discover its own
 * bundled providers that way).
 */
public class ArrowIpcSourceFactory implements ArrowSourceFactory {

    /** {@link ArrowIngestionConfig#DATASET_TYPE_PROP_KEY} value that selects this factory. */
    public static final String TYPE = "ARROW_IPC";

    /** Path to the local Arrow IPC (file format) file to read. */
    public static final String ARROW_IPC_PATH = "arrow_ipc_path";

    /** No-arg constructor. */
    public ArrowIpcSourceFactory() {}

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public ArrowSource createArrowSource(Map<String, Object> params, ArrowIngestionConfig ingestionConfig)
            throws IOException {
        String path = ConfigurationUtils.readStringProperty(params, ARROW_IPC_PATH);
        return ArrowIpcSource.fromPath(Path.of(path));
    }
}
