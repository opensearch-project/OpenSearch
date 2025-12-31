/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.composite;

import org.opensearch.index.engine.exec.coord.Segment;

import java.util.Collections;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicLong;

import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.FileInfos;
import org.opensearch.index.engine.exec.IndexingExecutionEngine;
import org.opensearch.index.engine.exec.Merger;
import org.opensearch.index.engine.exec.RefreshInput;
import org.opensearch.index.engine.exec.RefreshResult;
import org.opensearch.index.engine.exec.Writer;
import org.opensearch.index.engine.exec.coord.Any;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CompositeDataFormatWriterPool;
import org.opensearch.index.engine.exec.text.TextEngine;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.plugins.DataSourcePlugin;
import org.opensearch.plugins.PluginsService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CompositeIndexingExecutionEngine implements IndexingExecutionEngine<Any> {

    private final CompositeDataFormatWriterPool dataFormatWriterPool;
    private final Any dataFormat;
    private final AtomicLong writerGeneration;
    private final List<IndexingExecutionEngine<?>> delegates = new ArrayList<>();

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
        this.dataFormat = new Any(dataFormats, dataFormats.getFirst());
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

    public long getNextWriterGeneration() {
        return writerGeneration.getAndIncrement();
    }

    /**
     * Updates the writer generation counter to be at least minGeneration + 1.
     * This is used during replication/recovery to ensure the replica's writer generation
     * is always greater than any replicated file's generation, preventing file name collisions.
     *
     * @param minGeneration The minimum generation value from replicated files
     */
    public void updateWriterGenerationIfNeeded(long minGeneration) {
        writerGeneration.updateAndGet(current -> Math.max(current, minGeneration + 1));
    }

    /**
     * Gets the current writer generation without incrementing.
     *
     * @return The current writer generation value
     */
    public long getCurrentWriterGeneration() {
        return writerGeneration.get();
    }

    @Override
    public List<String> supportedFieldTypes() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void loadWriterFiles(CatalogSnapshot catalogSnapshot) throws IOException {
        for (IndexingExecutionEngine<?> delegate : delegates) {
            delegate.loadWriterFiles(catalogSnapshot);
        }
    }

    @Override
    public void deleteFiles(Map<String, Collection<String>> filesToDelete) throws IOException {
        for (IndexingExecutionEngine<?> delegate : delegates) {
            Map<String, Collection<String>> formatSpecificFilesToDelete = new HashMap<>();
            formatSpecificFilesToDelete.put(delegate.getDataFormat().name(), filesToDelete.get(delegate.getDataFormat().name()));
            delegate.deleteFiles(formatSpecificFilesToDelete);
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
        try {
            List<CompositeDataFormatWriter> dataFormatWriters = dataFormatWriterPool.checkoutAll();
            List<Segment> refreshedSegment = ignore.getExistingSegments();
            List<Segment> newSegmentList = new ArrayList<>();
            // flush to disk
            for (CompositeDataFormatWriter dataFormatWriter : dataFormatWriters) {
                Segment newSegment = new Segment(0);
                FileInfos fileInfos = dataFormatWriter.flush(null);
                fileInfos.getWriterFilesMap().forEach((key, value) -> {
                    newSegment.addSearchableFiles(key.name(), value);
                });
                dataFormatWriter.close();
                if (!newSegment.getDFGroupedSearchableFiles().isEmpty()) {
                    newSegmentList.add(newSegment);
                }
            }

            if (newSegmentList.isEmpty()) {
                return null;
            } else {
                refreshedSegment.addAll(newSegmentList);
            }

            // call refresh for delegats
            for (IndexingExecutionEngine<?> delegate : delegates) {
                delegate.refresh(new RefreshInput());
            }

            // make indexing engines aware of everything
            finalResult = new RefreshResult();
            finalResult.setRefreshedSegments(refreshedSegment);

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

    public List<IndexingExecutionEngine<?>> getDelegates() {
        return Collections.unmodifiableList(delegates);
    }

    public CompositeDataFormatWriterPool getDataFormatWriterPool() {
        return dataFormatWriterPool;
    }

    public long getNativeBytesUsed() {
        return delegates.stream().mapToLong(IndexingExecutionEngine::getNativeBytesUsed).sum();
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(delegates);
    }
}
