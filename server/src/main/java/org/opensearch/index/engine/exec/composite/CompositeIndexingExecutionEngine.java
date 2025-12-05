/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.composite;

import org.opensearch.index.engine.exec.engine.IndexingConfiguration;
import org.opensearch.index.engine.exec.format.DataSourcePlugin;
import org.opensearch.index.engine.exec.format.DataFormat;
import org.opensearch.index.engine.exec.engine.FileMetadata;
import org.opensearch.index.engine.exec.engine.IndexingExecutionEngine;
import org.opensearch.index.engine.exec.engine.RefreshInput;
import org.opensearch.index.engine.exec.engine.RefreshResult;
import org.opensearch.index.engine.exec.engine.Writer;
import org.opensearch.index.engine.exec.format.DataSourceRegistry;
import org.opensearch.index.engine.exec.manage.DocumentWriterPool;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * An engine which vends writers across multiple data formats, and manages operations like flush, refreshes, etc. across
 * all of them while maintaining generations of data.
 */
public class CompositeIndexingExecutionEngine implements IndexingExecutionEngine<CompositeDataFormat> {

    final DocumentWriterPool pool;
    private CompositeDataFormat dataFormat;
    public final List<IndexingExecutionEngine<?>> delegates = new ArrayList<>();

    public CompositeIndexingExecutionEngine(DataSourceRegistry dataSourceRegistry, IndexingConfiguration indexingConfiguration) {
        this.dataFormat = new CompositeDataFormat(indexingConfiguration.indexSettings().getSettings().getAsList("index.data_formats")
            .stream()
            .map(dataSourceRegistry::getDataFormat)
            .toList());

        for (DataFormat dataFormat : this.dataFormat.dataFormats()) {
            delegates.add(dataSourceRegistry.newEngine(dataFormat, indexingConfiguration));
        }

        this.pool = new DocumentWriterPool(() -> new CompositeDataFormatWriter(this));
    }

    @Override
    public DataFormat getDataFormat() {
        return dataFormat;
    }

    @Override
    public Writer<CompositeDataFormatWriter.CompositeDocumentInput> createWriter() throws IOException {
        return pool.fetchWriter();
    }

    @Override
    public RefreshResult refresh(RefreshInput ignore) {
        RefreshResult finalResult = new RefreshResult();
        Map<DataFormat, RefreshInput> refreshInputs = new HashMap<>();
        try {
            List<CompositeDataFormatWriter> dataFormatWriters = pool.freeAll();

            // flush to disk
            for (CompositeDataFormatWriter dataFormatWriter : dataFormatWriters) {
                FileMetadata metadata = dataFormatWriter.flush();
                refreshInputs.computeIfAbsent(metadata.df(), df -> new RefreshInput()).add(metadata);
            }

            // make indexing engines aware of everything
            for (IndexingExecutionEngine<?> delegate : delegates) {
                RefreshResult result = delegate.refresh(refreshInputs.get(delegate.getDataFormat()));
                finalResult.add(delegate.getDataFormat(), result.getRefreshedFiles().get(delegate.getDataFormat()));
            }

            // provide a view to the upper layer
            return finalResult;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public IndexingConfiguration indexingConfiguration() {
        return null;
    }
}
