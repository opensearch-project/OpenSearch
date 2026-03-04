/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.calcite.sql.SqlOperatorTable;
import org.opensearch.analytics.backend.EngineBridge;
import org.opensearch.analytics.spi.AnalyticsBackEndPlugin;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.SearchExecEngine;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.Plugin;

import java.io.IOException;
import java.util.List;

/**
 * Plugin providing the Lucene-based search execution engine.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneSearchEnginePlugin implements AnalyticsBackEndPlugin {

    @Override
    public String name() {
        return "lucene-analytics-backend";
    }

    @Override
    public EngineBridge<?, ?, ?> bridge() {
        return null;
    }

    @Override
    public SqlOperatorTable operatorTable() {
        return null;
    }

    @Override
    public SearchExecEngine<?, ?> create(ShardPath shardPath, DataFormat dataFormat) throws IOException {
        // TODO: obtain ReferenceManager from the shard's InternalEngine
        throw new UnsupportedOperationException("Lucene engine creation not yet wired to shard lifecycle");
    }

    @Override
    public List<DataFormat> getSupportedFormats() {
        return List.of();
    }
}
