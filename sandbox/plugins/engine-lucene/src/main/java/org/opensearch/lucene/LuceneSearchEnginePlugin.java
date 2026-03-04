/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.lucene;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.SearchExecEngine;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchEnginePlugin;

import java.io.IOException;

/**
 * Plugin providing the Lucene-based search execution engine.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneSearchEnginePlugin extends Plugin implements SearchEnginePlugin {

    @Override
    public SearchExecEngine<?, ?> createSearchExecEngine(ShardPath shardPath) throws IOException {
        // TODO: obtain ReferenceManager from the shard's InternalEngine
        throw new UnsupportedOperationException("Lucene engine creation not yet wired to shard lifecycle");
    }
}
