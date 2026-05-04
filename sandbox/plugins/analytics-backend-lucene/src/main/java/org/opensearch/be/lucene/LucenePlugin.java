/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.apache.lucene.index.DirectoryReader;
import org.opensearch.be.lucene.index.LuceneCommitter;
import org.opensearch.be.lucene.index.LuceneCommitterFactory;
import org.opensearch.be.lucene.index.LuceneIndexingExecutionEngine;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatPlugin;
import org.opensearch.index.engine.dataformat.IndexingEngineConfig;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.ReaderManagerConfig;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.commit.CommitterFactory;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.SearchBackEndPlugin;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

/**
 * Plugin providing Lucene as a data format, search back-end, and committer
 * for the composite engine.
 * <p>
 * Implements three plugin interfaces:
 * <ul>
 *   <li>{@link DataFormatPlugin} — registers Lucene as a data format that can write
 *       inverted indices for text fields via {@link LuceneIndexingExecutionEngine}</li>
 *   <li>{@link SearchBackEndPlugin} — provides {@link LuceneReaderManager} for search</li>
 *   <li>{@link EnginePlugin} — provides {@link LuceneCommitterFactory} for durable commits</li>
 * </ul>
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class LucenePlugin extends Plugin implements DataFormatPlugin, SearchBackEndPlugin<DirectoryReader>, EnginePlugin {

    private static final LuceneDataFormat DATA_FORMAT = new LuceneDataFormat();

    /** Creates a new LucenePlugin. */
    public LucenePlugin() {}

    // --- DataFormatPlugin ---

    /** {@inheritDoc} Returns the singleton {@link LuceneDataFormat} descriptor. */
    @Override
    public DataFormat getDataFormat() {
        return DATA_FORMAT;
    }

    /**
     * Creates a {@link LuceneIndexingExecutionEngine} for the given configuration.
     * Requires the committer to be a {@link LuceneCommitter}.
     *
     * @param indexingEngineConfig the engine configuration containing committer, mapper service, and store
     * @return a new Lucene indexing execution engine
     * @throws IllegalStateException if the committer is not a {@link LuceneCommitter}
     */
    @Override
    public IndexingExecutionEngine<?, ?> indexingEngine(IndexingEngineConfig indexingEngineConfig) {
        Committer committer = indexingEngineConfig.committer();
        if (committer instanceof LuceneCommitter luceneCommitter) {
            return new LuceneIndexingExecutionEngine(
                DATA_FORMAT,
                luceneCommitter,
                indexingEngineConfig.mapperService(),
                indexingEngineConfig.store()
            );
        }
        throw new IllegalStateException(
            "LuceneIndexingExecutionEngine requires a LuceneCommitter but got: "
                + (committer != null ? committer.getClass().getName() : "null")
        );
    }

    // --- SearchBackEndPlugin ---

    /** {@inheritDoc} Returns {@code "lucene"}. */
    @Override
    public String name() {
        return LuceneDataFormat.LUCENE_FORMAT_NAME;
    }

    /** {@inheritDoc} Returns a singleton list containing the Lucene data format. */
    @Override
    public List<String> getSupportedFormats() {
        return List.of(LuceneDataFormat.LUCENE_FORMAT_NAME);
    }

    /**
     * Creates a {@link LuceneReaderManager} for the given settings by delegating to
     * {@link LuceneSearchBackEnd#createReaderManager}.
     *
     * @param settings the reader manager configuration
     * @return a new reader manager
     * @throws IOException if reader creation fails
     */
    @Override
    public EngineReaderManager<DirectoryReader> createReaderManager(ReaderManagerConfig settings) throws IOException {
        return LuceneSearchBackEnd.createReaderManager(settings);
    }

    // --- EnginePlugin ---

    /**
     * Returns a {@link LuceneCommitterFactory} for creating {@link LuceneCommitter} instances.
     *
     * @param indexSettings the index settings (unused)
     * @return an optional containing the factory
     */
    @Override
    public Optional<CommitterFactory> getCommitterFactory(IndexSettings indexSettings) {
        return Optional.of(new LuceneCommitterFactory());
    }
}
