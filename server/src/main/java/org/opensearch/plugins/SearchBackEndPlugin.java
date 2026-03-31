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
import org.opensearch.index.shard.ShardPath;

import java.io.IOException;
import java.util.List;

/**
 * Interface for back-end query engines.
 *
 * @opensearch.internal
 */
public interface SearchBackEndPlugin {

    String name();

    List<DataFormat> getSupportedFormats();

    EngineReaderManager<?> createReaderManager(DataFormat format, ShardPath shardPath) throws IOException;
}
