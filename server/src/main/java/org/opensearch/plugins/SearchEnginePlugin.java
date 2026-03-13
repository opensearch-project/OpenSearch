/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.SearchExecEngine;
import org.opensearch.index.shard.ShardPath;

import java.io.IOException;

/**
 * Plugin SPI for providing pluggable search execution engines.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface SearchEnginePlugin {

    SearchExecEngine<?, ?> createSearchExecEngine(ShardPath shardPath) throws IOException;
}
