/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.index.DirectoryReader;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.ReaderManagerConfig;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.commit.CommitterConfig;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchBackEndPlugin;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Plugin providing Lucene as both a search back-end and a committer
 * for the composite engine.
 * <p>
 * Delegates to {@link LuceneSearchBackEnd} for reader management
 * and {@link LuceneEnginePlugin} for committer creation.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LucenePlugin extends Plugin implements SearchBackEndPlugin<DirectoryReader>, EnginePlugin {

    /** Creates a new LucenePlugin. */
    public LucenePlugin() {}

    @Override
    public String name() {
        return "lucene";
    }

    @Override
    public List<DataFormat> getSupportedFormats() {
        return List.of();
    }

    @Override
    public EngineReaderManager<DirectoryReader> createReaderManager(ReaderManagerConfig settings) throws IOException {
        return LuceneSearchBackEnd.createReaderManager(settings);
    }

    @Override
    public Optional<Committer> getCommitter(CommitterConfig committerConfig) throws IOException {
        return Optional.of(LuceneEnginePlugin.createCommitter(committerConfig));
    }
}
