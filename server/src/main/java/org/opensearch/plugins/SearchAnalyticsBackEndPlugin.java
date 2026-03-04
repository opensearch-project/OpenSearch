/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.SearchExecEngine;
import org.opensearch.index.shard.ShardPath;

import java.io.IOException;
import java.util.List;

/**
 * SPI extension point for back-end query engines (DataFusion, Lucene, etc.).
 * @opensearch.internal
 */
public interface SearchAnalyticsBackEndPlugin {
    String name();

    SearchExecEngine<?, ?> create(ShardPath shardPath, DataFormat dataFormat) throws IOException;;

    List<DataFormat> getSupportedFormats();
}

