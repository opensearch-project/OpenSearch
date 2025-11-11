/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.composite;

import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.FileInfos;
import org.opensearch.index.engine.exec.IndexingExecutionEngine;
import org.opensearch.index.engine.exec.Merger;
import org.opensearch.index.engine.exec.RefreshInput;
import org.opensearch.index.engine.exec.RefreshResult;
import org.opensearch.index.engine.exec.Writer;
import org.opensearch.index.engine.exec.coord.Any;
import org.opensearch.index.engine.exec.coord.CompositeDataFormatWriterPool;
import org.opensearch.index.engine.exec.text.TextEngine;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.DataSourcePlugin;
import org.opensearch.plugins.PluginsService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

public class CompositeIndexingExecutionEngine implements IndexingExecutionEngine<Any> {

    private final CompositeDataFormatWriterPool dataFormatWriterPool;
    private final Any dataFormat;
    private final AtomicLong writerGeneration;
    private final List<IndexingExecutionEngine<?>> delegates = new ArrayList<>();

    public CompositeIndexingExecutionEngine(
        MapperService mapperService,
        PluginsService pluginsService,
        Any dataformat,
        ShardPath shardPath,
        long initialWriterGeneration
    ) {
        this.dataFormat = dataformat;
        this.writerGeneration = new AtomicLong(initialWriterGeneration);
        try {
            for (DataFormat dataFormat : dataformat.getDataFormats()) {
                DataSourcePlugin plugin = pluginsService.filterPlugins(DataSourcePlugin.class)
                    .stream()
                    .filter(curr -> curr.getDataFormat().equals(dataFormat))
                    .findFirst()
                    .orElseThrow(() -> new IllegalArgumentException("dataformat [" + dataFormat + "] is not registered."));
                delegates.add(plugin.indexingEngine(mapperService, shardPath));
            }
        } catch (NullPointerException e) {
            // my own testing
            delegates.add(new TextEngine());
        }
        this.dataFormatWriterPool =
            new CompositeDataFormatWriterPool(
                () -> new CompositeDataFormatWriter(this, writerGeneration.getAndIncrement()),
                LinkedList::new,
                Runtime.getRuntime().availableProcessors()
            );
    }

    public CompositeIndexingExecutionEngine(
        MapperService mapperService,
        PluginsService pluginsService,
        ShardPath shardPath,
        long initialWriterGeneration
    ) {
        this.writerGeneration = new AtomicLong(initialWriterGeneration);
        List<DataFormat> dataFormats = new ArrayList<>();
        try {
            DataSourcePlugin plugin = pluginsService.filterPlugins(DataSourcePlugin.class)
                .stream()
                .findAny()
                .orElseThrow(() -> new IllegalArgumentException("dataformat [" + DataFormat.TEXT + "] is not registered."));
            dataFormats.add(plugin.getDataFormat());
            delegates.add(plugin.indexingEngine(mapperService, shardPath));
        } catch (NullPointerException e) {
            delegates.add(new TextEngine());
        }
        this.dataFormat = new Any(dataFormats, dataFormats.get(0));
        this.dataFormatWriterPool =
            new CompositeDataFormatWriterPool(
                () -> new CompositeDataFormatWriter(this, writerGeneration.getAndIncrement()),
                LinkedList::new,
                Runtime.getRuntime().availableProcessors()
            );
    }

    @Override
    public Any getDataFormat() {
        return dataFormat;
    }

    @Override
    public List<String> supportedFieldTypes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void loadWriterFiles(ShardPath shardPath) throws IOException {
        for (IndexingExecutionEngine<?> delegate : delegates) {
            delegate.loadWriterFiles(shardPath);
        }
    }

    @Override
    public Writer<CompositeDataFormatWriter.CompositeDocumentInput> createWriter(long generation) throws IOException {
        throw new UnsupportedOperationException();
    }

    public Writer<CompositeDataFormatWriter.CompositeDocumentInput> createCompositeWriter() {
        return dataFormatWriterPool.getAndLock();
    }

    @Override
    public RefreshResult refresh(RefreshInput ignore) throws IOException {
        RefreshResult finalResult;
        Map<DataFormat, RefreshInput> refreshInputs = new HashMap<>();
        try {
            List<CompositeDataFormatWriter> dataFormatWriters = dataFormatWriterPool.checkoutAll();

            // flush to disk
            for (CompositeDataFormatWriter dataFormatWriter : dataFormatWriters) {
                FileInfos fileInfos = dataFormatWriter.flush(null);
                fileInfos.getWriterFilesMap()
                    .forEach((key, value) -> refreshInputs.computeIfAbsent(key, dataFormat -> new RefreshInput()).add(value));
                dataFormatWriter.close();
            }

            if (refreshInputs.isEmpty()) {
                return null;
            }

            // make indexing engines aware of everything
            finalResult = refresh(refreshInputs);

            // provide a view to the upper layer
            return finalResult;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    @Override
    public Merger getMerger() {
        throw new UnsupportedOperationException("Merger for Composite Engine is not used");
    }

    public RefreshResult refresh(Map<DataFormat, RefreshInput> refreshInputs) throws IOException {
        RefreshResult finalResult = new RefreshResult();

        // make indexing engines aware of everything
        for (IndexingExecutionEngine<?> delegate : delegates) {
            RefreshInput refreshInput = refreshInputs.get(delegate.getDataFormat());
            if (refreshInput != null) {
                RefreshResult result = delegate.refresh(refreshInput);
                finalResult.add(delegate.getDataFormat(), result.getRefreshedFiles(delegate.getDataFormat()));
            }
        }
        return finalResult;
    }

    public List<IndexingExecutionEngine<?>> getDelegates() {
        return Collections.unmodifiableList(delegates);
    }

    public CompositeDataFormatWriterPool getDataFormatWriterPool() {
        return dataFormatWriterPool;
    }
}
