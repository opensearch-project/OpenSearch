/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.queue.LockablePool;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatPlugin;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.Merger;
import org.opensearch.index.engine.dataformat.RefreshInput;
import org.opensearch.index.engine.dataformat.RefreshResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.shard.ShardPath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A composite {@link IndexingExecutionEngine} that orchestrates indexing across
 * multiple per-format engines behind a single interface.
 * <p>
 * The engine delegates writer creation, refresh, file deletion, and document input
 * creation to each per-format engine. A primary engine is designated based on the
 * configured primary format name and is used for merge operations.
 * <p>
 * The composite {@link DataFormat} exposed by this engine represents the union of
 * all per-format supported field type capabilities.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CompositeIndexingExecutionEngine implements IndexingExecutionEngine<CompositeDataFormat, CompositeDocumentInput> {

    private static final Logger logger = LogManager.getLogger(CompositeIndexingExecutionEngine.class);

    private final IndexingExecutionEngine<?, ?> primaryEngine;
    private final Set<IndexingExecutionEngine<?, ?>> secondaryEngines;
    private final CompositeDataFormat compositeDataFormat;
    private final LockablePool<CompositeWriter> writerPool;

    /**
     * Constructs a CompositeIndexingExecutionEngine by reading index settings to
     * determine the primary and secondary data formats, validating that all configured
     * formats are registered, and creating per-format engines via the discovered
     * {@link DataFormatPlugin} instances.
     * <p>
     * The primary engine is the authoritative format used for merge operations and
     * commit coordination. Secondary engines receive writes alongside the primary but
     * are not used as the merge authority.
     * <p>
     * The writer pool is created internally and initialized with a writer supplier
     * that creates {@link CompositeWriter} instances bound to this engine.
     *
     * @param dataFormatPlugins the discovered data format plugins keyed by format name
     * @param indexSettings the index settings containing composite configuration
     * @param mapperService the mapper service for field mapping resolution
     * @param shardPath the shard path for file storage
     * @throws IllegalArgumentException if any configured format is not registered
     */
    public CompositeIndexingExecutionEngine(
        Map<String, DataFormatPlugin> dataFormatPlugins,
        IndexSettings indexSettings,
        MapperService mapperService,
        ShardPath shardPath
    ) {
        Objects.requireNonNull(dataFormatPlugins, "dataFormatPlugins must not be null");
        Objects.requireNonNull(indexSettings, "indexSettings must not be null");

        Settings settings = indexSettings.getSettings();

        String primaryFormatName = CompositeEnginePlugin.PRIMARY_DATA_FORMAT.get(settings);
        List<String> secondaryFormatNames = CompositeEnginePlugin.SECONDARY_DATA_FORMATS.get(settings);

        validateFormatsRegistered(dataFormatPlugins, primaryFormatName, secondaryFormatNames);

        List<DataFormat> allFormats = new ArrayList<>();
        DataFormatPlugin primaryPlugin = dataFormatPlugins.get(primaryFormatName);
        this.primaryEngine = primaryPlugin.indexingEngine(mapperService, shardPath, indexSettings);
        allFormats.add(primaryPlugin.getDataFormat());

        List<IndexingExecutionEngine<?, ?>> secondaries = new ArrayList<>();
        for (String secondaryName : secondaryFormatNames) {
            DataFormatPlugin secondaryPlugin = dataFormatPlugins.get(secondaryName);
            secondaries.add(secondaryPlugin.indexingEngine(mapperService, shardPath, indexSettings));
            allFormats.add(secondaryPlugin.getDataFormat());
        }
        this.secondaryEngines = Set.copyOf(secondaries);

        this.compositeDataFormat = new CompositeDataFormat(allFormats);

        // Create the writer pool internally, matching the reference code pattern
        AtomicLong writerGenerationCounter = new AtomicLong(0);
        this.writerPool = new LockablePool<>(
            () -> new CompositeWriter(this, writerGenerationCounter.getAndIncrement()),
            LinkedList::new,
            Runtime.getRuntime().availableProcessors()
        );
    }

    /**
     * Validates that the primary and all secondary data format plugins are registered.
     *
     * @param dataFormatPlugins the discovered data format plugins keyed by format name
     * @param primaryFormatName the configured primary format name
     * @param secondaryFormatNames the configured secondary format names
     * @throws IllegalArgumentException if any configured format is not registered
     */
    static void validateFormatsRegistered(
        Map<String, DataFormatPlugin> dataFormatPlugins,
        String primaryFormatName,
        List<String> secondaryFormatNames
    ) {
        if (primaryFormatName == null || primaryFormatName.isBlank()) {
            throw new IllegalArgumentException("Primary data format name must not be null or blank");
        }
        if (dataFormatPlugins.containsKey(primaryFormatName) == false) {
            throw new IllegalArgumentException(
                "Primary data format ["
                    + primaryFormatName
                    + "] is not registered on this node. Available formats: "
                    + dataFormatPlugins.keySet()
            );
        }
        for (String secondaryName : secondaryFormatNames) {
            if (secondaryName == null || secondaryName.isBlank()) {
                throw new IllegalArgumentException("Secondary data format name must not be null or blank");
            }
            if (secondaryName.equals(primaryFormatName)) {
                throw new IllegalStateException(
                    "Secondary data format [" + secondaryName + "] is the same as primary :[" + primaryFormatName + "]"
                );
            }
            if (dataFormatPlugins.containsKey(secondaryName) == false) {
                throw new IllegalArgumentException(
                    "Secondary data format ["
                        + secondaryName
                        + "] is not registered on this node. Available formats: "
                        + dataFormatPlugins.keySet()
                );
            }
        }
    }

    @Override
    public Writer<CompositeDocumentInput> createWriter(long writerGeneration) {
        return new CompositeWriter(this, writerGeneration);
    }

    @Override
    public Merger getMerger() {
        return primaryEngine.getMerger();
    }

    @Override
    public RefreshResult refresh(RefreshInput refreshInput) throws IOException {
        List<CompositeWriter> dataFormatWriters = writerPool.checkoutAll();

        // Mark each writer as flush-pending before flushing
        for (CompositeWriter writer : dataFormatWriters) {
            writer.setFlushPending();
        }

        List<Segment> refreshedSegments = new ArrayList<>(refreshInput.existingSegments());
        List<Segment> newSegmentList = new ArrayList<>();

        logger.debug(
            "Refreshing composite engine: flushing {} writers, existing segments={}",
            dataFormatWriters.size(),
            refreshedSegments.size()
        );

        // Flush each writer to disk and build segments from the file infos
        for (CompositeWriter writer : dataFormatWriters) {
            FileInfos fileInfos = writer.flush();
            Segment.Builder segmentBuilder = Segment.builder(writer.getWriterGeneration());
            boolean hasFiles = false;
            for (Map.Entry<DataFormat, WriterFileSet> entry : fileInfos.writerFilesMap().entrySet()) {
                logger.debug(
                    "Writer gen={} flushed format=[{}] files={}",
                    writer.getWriterGeneration(),
                    entry.getKey().name(),
                    entry.getValue().files()
                );
                segmentBuilder.addSearchableFiles(entry.getKey(), entry.getValue());
                hasFiles = true;
            }
            writer.close();
            if (hasFiles) {
                newSegmentList.add(segmentBuilder.build());
            }
        }

        if (newSegmentList.isEmpty()) {
            logger.debug("No new segments produced from flush");
            return null;
        }

        logger.debug("Produced {} new segments from flush", newSegmentList.size());
        refreshedSegments.addAll(newSegmentList);

        // Delegate refresh to each per-format engine
        RefreshInput emptyInput = RefreshInput.builder().build();
        primaryEngine.refresh(emptyInput);
        for (IndexingExecutionEngine<?, ?> engine : secondaryEngines) {
            engine.refresh(emptyInput);
        }

        return new RefreshResult(refreshedSegments);
    }

    @Override
    public CompositeDataFormat getDataFormat() {
        return compositeDataFormat;
    }

    @Override
    public long getNativeBytesUsed() {
        long total = primaryEngine.getNativeBytesUsed();
        for (IndexingExecutionEngine<?, ?> engine : secondaryEngines) {
            total += engine.getNativeBytesUsed();
        }
        return total;
    }

    @Override
    public void deleteFiles(Map<String, Collection<String>> filesToDelete) throws IOException {
        IOException firstException = null;
        try {
            primaryEngine.deleteFiles(filesToDelete);
        } catch (IOException e) {
            logger.error("Failed to delete files in primary engine [{}]: {}", primaryEngine.getDataFormat().name(), e.getMessage());
            firstException = e;
        }
        for (IndexingExecutionEngine<?, ?> engine : secondaryEngines) {
            try {
                engine.deleteFiles(filesToDelete);
            } catch (IOException e) {
                logger.error("Failed to delete files in secondary engine [{}]: {}", engine.getDataFormat().name(), e.getMessage());
                if (firstException == null) {
                    firstException = e;
                } else {
                    firstException.addSuppressed(e);
                }
            }
        }
        if (firstException != null) {
            throw firstException;
        }
    }

    @Override
    public CompositeDocumentInput newDocumentInput() {
        DocumentInput<?> primaryInput = primaryEngine.newDocumentInput();
        Map<DataFormat, DocumentInput<?>> secondaryInputMap = new IdentityHashMap<>();
        for (IndexingExecutionEngine<?, ?> engine : secondaryEngines) {
            secondaryInputMap.put(engine.getDataFormat(), engine.newDocumentInput());
        }
        return new CompositeDocumentInput(primaryEngine.getDataFormat(), primaryInput, secondaryInputMap);
    }

    /**
     * Returns the primary delegate engine.
     *
     * @return the primary engine
     */
    public IndexingExecutionEngine<?, ?> getPrimaryDelegate() {
        return primaryEngine;
    }

    /**
     * Returns the secondary delegate engines.
     *
     * @return the secondary engines
     */
    public Set<IndexingExecutionEngine<?, ?>> getSecondaryDelegates() {
        return secondaryEngines;
    }

}
