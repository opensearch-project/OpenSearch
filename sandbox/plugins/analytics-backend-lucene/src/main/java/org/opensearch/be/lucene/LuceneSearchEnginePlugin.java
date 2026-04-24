/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.ReaderManagerConfig;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.IndexFilterTreeProvider;
import org.opensearch.common.lucene.index.OpenSearchDirectoryReader;
import org.opensearch.plugins.SearchBackEndPlugin;

import java.io.IOException;
import java.util.List;

/**
 * Plugin providing Lucene as an index filter or source provider.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LuceneSearchEnginePlugin implements SearchBackEndPlugin<OpenSearchDirectoryReader> {

    /** Creates a new LuceneSearchEnginePlugin. */
    public LuceneSearchEnginePlugin() {}

    @Override
    public String name() {
        return "lucene-analytics-backend";
    }

    @Override
    public EngineReaderManager<OpenSearchDirectoryReader> createReaderManager(ReaderManagerConfig settings) throws IOException {
        return LuceneSearchBackEnd.createReaderManager(settings);
    }

    /**
     * Creates a new {@link LuceneIndexFilterTreeProvider} for boolean tree queries.
     *
     * @return a tree provider that delegates to {@link LuceneIndexFilterProvider}
     */
    public IndexFilterTreeProvider<?, ?, ?> createTreeProvider() {
        return new LuceneIndexFilterTreeProvider();
    }

    @Override
    public List<DataFormat> getSupportedFormats() {
        return List.of();
    }
}
