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
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatPlugin;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.IndexingEngineConfig;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.Merger;
import org.opensearch.index.engine.dataformat.RefreshInput;
import org.opensearch.index.engine.dataformat.RefreshResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.composite.merge.CompositeMerger;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.commit.IndexStoreProvider;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.store.FormatChecksumStrategy;
import org.opensearch.index.store.Store;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

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
    private final Committer committer;

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
     * @param indexSettings       the index settings containing composite configuration
     * @param mapperService       the mapper service for field mapping resolution
     * @param committer           the committer for durable catalog snapshot persistence during flush
     * @param dataFormatRegistry registry containing information about available data formats on the node
     * @param store store for the current index
     * @param checksumStrategies  per-format checksum strategies from the directory, keyed by format name.
     *                            May be null or empty if the directory is not yet available.
     * @throws IllegalArgumentException if any configured format is not registered
     * @throws IllegalStateException if committer is null
     */
    public CompositeIndexingExecutionEngine(
        IndexSettings indexSettings,
        MapperService mapperService,
        Committer committer,
        DataFormatRegistry dataFormatRegistry,
        Store store,
        Map<String, FormatChecksumStrategy> checksumStrategies
    ) {
        Objects.requireNonNull(indexSettings, "indexSettings must not be null");
        if (committer == null) {
            throw new IllegalStateException("Committer must not be null");
        }

        Settings settings = indexSettings.getSettings();

        String primaryFormatName = CompositeDataFormatPlugin.PRIMARY_DATA_FORMAT.get(settings);
        List<String> secondaryFormatNames = CompositeDataFormatPlugin.SECONDARY_DATA_FORMATS.get(settings);

        validateFormatsRegistered(dataFormatRegistry, primaryFormatName, secondaryFormatNames);

        Map<String, FormatChecksumStrategy> strategies = checksumStrategies != null ? checksumStrategies : Map.of();
        IndexingEngineConfig engineSettings = new IndexingEngineConfig(committer, mapperService, indexSettings, store, dataFormatRegistry);

        List<DataFormat> allFormats = new ArrayList<>();
        DataFormat primaryFormat = dataFormatRegistry.format(primaryFormatName);
        this.primaryEngine = dataFormatRegistry.getIndexingEngine(engineSettings, primaryFormat);
        allFormats.add(primaryFormat);

        List<IndexingExecutionEngine<?, ?>> secondaries = new ArrayList<>();
        for (String secondaryName : secondaryFormatNames) {
            DataFormat secondaryFormat = dataFormatRegistry.format(secondaryName);
            secondaries.add(dataFormatRegistry.getIndexingEngine(engineSettings, secondaryFormat));
            allFormats.add(secondaryFormat);
        }
        this.secondaryEngines = Set.copyOf(secondaries);

        this.compositeDataFormat = new CompositeDataFormat(primaryFormat, allFormats);
        this.committer = committer;
    }

    /**
     * Validates that the primary and all secondary data format plugins are registered.
     *
     * @param registry the discovered data format plugins keyed by format name
     * @param primaryFormatName the configured primary format name
     * @param secondaryFormatNames the configured secondary format names
     * @throws IllegalArgumentException if any configured format is not registered
     */
    static void validateFormatsRegistered(DataFormatRegistry registry, String primaryFormatName, List<String> secondaryFormatNames) {
        validateFormatIsRegistered(registry, primaryFormatName);
        for (String secondaryName : secondaryFormatNames) {
            validateFormatIsRegistered(registry, secondaryName);
            if (secondaryName.equals(primaryFormatName)) {
                throw new IllegalStateException(
                    "Secondary data format [" + secondaryName + "] is the same as primary :[" + primaryFormatName + "]"
                );
            }
        }
    }

    private static void validateFormatIsRegistered(DataFormatRegistry registry, String dataFormatName) {
        if (dataFormatName == null || dataFormatName.isBlank()) {
            throw new IllegalArgumentException("Primary data format name must not be null or blank");
        }
        if (registry.format(dataFormatName) == null) {
            throw new IllegalArgumentException(
                "Primary data format ["
                    + dataFormatName
                    + "] is not registered on this node. Available formats: "
                    + registry.getRegisteredFormats()
            );
        }
    }

    @Override
    public Writer<CompositeDocumentInput> createWriter(long writerGeneration) {
        return new CompositeWriter(this, writerGeneration);
    }

    @Override
    public Merger getMerger() {
        return new CompositeMerger(this, compositeDataFormat);
    }

    @Override
    public RefreshResult refresh(RefreshInput refreshInput) throws IOException {
        RefreshResult primary = primaryEngine.refresh(refreshInput);
        List<RefreshResult> secResults = new ArrayList<>();
        for (IndexingExecutionEngine<?, ?> engine : secondaryEngines) {
            secResults.add(engine.refresh(refreshInput));
        }

        Map<Long, Segment.Builder> mergedByGen = new LinkedHashMap<>();
        buildSegment(primary, mergedByGen);
        for (RefreshResult secResult : secResults) {
            buildSegment(secResult, mergedByGen);
        }

        List<Segment> merged = new ArrayList<>(mergedByGen.size());
        for (Segment.Builder builder : mergedByGen.values()) {
            merged.add(builder.build());
        }
        return new RefreshResult(merged);
    }

    private void buildSegment(RefreshResult primary, Map<Long, Segment.Builder> mergedByGen) {
        for (Segment seg : primary.refreshedSegments()) {
            Segment.Builder builder = mergedByGen.computeIfAbsent(seg.generation(), Segment::builder);
            for (Map.Entry<String, WriterFileSet> entry : seg.dfGroupedSearchableFiles().entrySet()) {
                builder.addSearchableFiles(entry.getKey(), entry.getValue());
            }
        }
    }

    @Override
    public long getNextWriterGeneration() {
        throw new UnsupportedOperationException();
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

    @Override
    public void close() throws IOException {
        IOUtils.closeWhileHandlingException(primaryEngine);
        secondaryEngines.forEach(IOUtils::closeWhileHandlingException);
        IOUtils.closeWhileHandlingException(committer);
    }

    /**
     * Returns the primary delegate engine.
     *
     * @return the primary engine
     */
    public IndexingExecutionEngine<?, ?> getPrimaryDelegate() {
        return primaryEngine;
    }

    @Override
    public IndexStoreProvider getProvider() {
        return primaryEngine.getProvider();
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
