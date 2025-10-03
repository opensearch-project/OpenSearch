/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.composite;

import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.IndexingExecutionEngine;
import org.opensearch.index.engine.exec.RefreshInput;
import org.opensearch.index.engine.exec.RefreshResult;
import org.opensearch.index.engine.exec.Writer;
import org.opensearch.index.engine.exec.coord.Any;
import org.opensearch.index.engine.exec.coord.DocumentWriterPool;
import org.opensearch.index.engine.exec.text.TextEngine;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.DataSourcePlugin;
import org.opensearch.plugins.PluginsService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CompositeIndexingExecutionEngine implements IndexingExecutionEngine<Any> {

    final DocumentWriterPool pool;
    private DataFormat dataFormat;
    public final List<IndexingExecutionEngine<?>> delegates = new ArrayList<>();

    public CompositeIndexingExecutionEngine(MapperService mapperService, PluginsService pluginsService, Any dataformat, ShardPath shardPath) {
        this.dataFormat = dataformat;
        try {
            for (DataFormat dataFormat : dataformat.getDataFormats()) {

                DataSourcePlugin plugin = pluginsService.filterPlugins(DataSourcePlugin.class).stream()
                    .filter(curr -> curr.getDataFormat().equals(dataFormat.name()))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("dataformat [" + dataFormat + "] is not registered."));
                delegates.add(plugin.indexingEngine(mapperService, shardPath));
            }
        } catch (NullPointerException e) {
            // my own testing
            delegates.add(new TextEngine());
        }
        this.pool = new DocumentWriterPool(() -> new CompositeDataFormatWriter(this));
    }

    public CompositeIndexingExecutionEngine(MapperService mapperService, PluginsService pluginsService, ShardPath shardPath) {
     try {
        DataSourcePlugin plugin = pluginsService.filterPlugins(DataSourcePlugin.class).stream()
            .findAny()
            .orElseThrow(() -> new IllegalArgumentException("dataformat [" + DataFormat.TEXT + "] is not registered."));
         delegates.add(plugin.indexingEngine(mapperService, shardPath));
     } catch (NullPointerException e) {
         delegates.add(new TextEngine());
     }
     this.pool = new DocumentWriterPool(() -> new CompositeDataFormatWriter(this));
    }

    @Override
    public DataFormat getDataFormat() {
        return dataFormat;
    }

    @Override
    public List<String> supportedFieldTypes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Writer<CompositeDataFormatWriter.CompositeDocumentInput> createWriter() throws IOException {
        return pool.fetchWriter();
    }

    @Override
    public RefreshResult refresh(RefreshInput ignore) throws IOException {
        RefreshResult finalResult = new RefreshResult();
        Map<DataFormat, RefreshInput> refreshInputs = new HashMap<>();
        try {
            List<CompositeDataFormatWriter> dataFormatWriters = pool.freeAll();

            // flush to disk
            for (CompositeDataFormatWriter dataFormatWriter : dataFormatWriters) {
                FileMetadata metadata = dataFormatWriter.flush(null);
                refreshInputs.computeIfAbsent(metadata.df(), df -> new RefreshInput()).add(metadata);
            }

            if (refreshInputs.isEmpty()) {
                return null;
            }

            // make indexing engines aware of everything
            for (IndexingExecutionEngine<?> delegate : delegates) {
                RefreshInput refreshInput = refreshInputs.get(delegate.getDataFormat());
                if (refreshInput != null) {
                    RefreshResult result = delegate.refresh(refreshInput);
                    finalResult.add(delegate.getDataFormat(), result.getRefreshedFiles().get(delegate.getDataFormat()));
                }
            }

            // provide a view to the upper layer
            return finalResult;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }
}
