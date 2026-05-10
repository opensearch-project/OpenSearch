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
import org.opensearch.composite.merge.CompositeMerger;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatPlugin;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.IndexingEngineConfig;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.Merger;
import org.opensearch.index.engine.dataformat.ReaderManagerConfig;
import org.opensearch.index.engine.dataformat.RefreshInput;
import org.opensearch.index.engine.dataformat.RefreshResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.commit.IndexStoreProvider;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.store.FormatChecksumStrategy;
import org.opensearch.index.store.Store;

import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

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
        IndexingEngineConfig engineSettings = new IndexingEngineConfig(
            committer,
            mapperService,
            indexSettings,
            store,
            dataFormatRegistry,
            strategies
        );

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

    /**
     * Creates a {@link CompositeWriter} that fans out writes to all per-format engines.
     *
     * @param writerGeneration the generation number for the new writer
     * @return a composite writer bound to this engine
     */
    @Override
    public Writer<CompositeDocumentInput> createWriter(long writerGeneration) {
        return new CompositeWriter(this, writerGeneration);
    }

    /** {@inheritDoc} Delegates to the primary engine's merger. */
    @Override
    public Merger getMerger() {
        return new CompositeMerger(this, compositeDataFormat);
    }

    /**
     * Refreshes all per-format engines and merges their results into a unified list of
     * {@link Segment} instances. Each segment groups its per-format {@link WriterFileSet}
     * entries by writer generation.
     *
     * @param refreshInput the refresh input containing existing and new segments
     * @return the merged refresh result across all formats
     * @throws IOException if any per-format refresh fails
     */
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

    /**
     * Not supported — writer generation is managed by the {@code DataFormatAwareEngine}.
     *
     * @throws UnsupportedOperationException always
     */
    @Override
    public long getNextWriterGeneration() {
        throw new UnsupportedOperationException();
    }

    /** {@inheritDoc} Returns the composite data format representing all per-format capabilities. */
    @Override
    public CompositeDataFormat getDataFormat() {
        return compositeDataFormat;
    }

    /**
     * Returns the total native memory bytes used across all per-format engines.
     *
     * @return the sum of native bytes used by primary and secondary engines
     */
    @Override
    public long getNativeBytesUsed() {
        long total = primaryEngine.getNativeBytesUsed();
        for (IndexingExecutionEngine<?, ?> engine : secondaryEngines) {
            total += engine.getNativeBytesUsed();
        }
        return total;
    }

    /**
     * Deletes files from all per-format engines. If any engine fails, the first exception
     * is thrown with subsequent failures added as suppressed exceptions.
     *
     * @param filesToDelete map of format name to collection of file names to delete
     * @throws IOException if any per-format engine fails to delete files
     */
    @Override
    public Map<String, Collection<String>> deleteFiles(Map<String, Collection<String>> filesToDelete) throws IOException {
        Map<String, Collection<String>> allFailed = new HashMap<>();
        IOException firstException = null;
        try {
            Map<String, Collection<String>> failed = primaryEngine.deleteFiles(filesToDelete);
            failed.forEach((k, v) -> allFailed.computeIfAbsent(k, x -> new ArrayList<>()).addAll(v));
        } catch (IOException e) {
            logger.error("Failed to delete files in primary engine [{}]: {}", primaryEngine.getDataFormat().name(), e.getMessage());
            firstException = e;
        }
        for (IndexingExecutionEngine<?, ?> engine : secondaryEngines) {
            try {
                Map<String, Collection<String>> failed = engine.deleteFiles(filesToDelete);
                failed.forEach((k, v) -> allFailed.computeIfAbsent(k, x -> new ArrayList<>()).addAll(v));
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
        return allFailed;
    }

    /**
     * Creates a {@link CompositeDocumentInput} that fans out field additions to the primary
     * and all secondary per-format document inputs.
     *
     * @return a new composite document input
     */
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
     * Closes all per-format engines and the committer. Exceptions from secondary engines
     * are handled gracefully to ensure all resources are released.
     *
     * @throws IOException if closing the primary engine or committer fails
     */
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

    /**
     * Returns a composite {@link IndexStoreProvider} that delegates to each per-format
     * engine's provider based on the requested data format.
     *
     * @return the composite store provider
     */
    @Override
    public IndexStoreProvider getProvider() {
        return new IndexStoreProvider() {
            private final Map<DataFormat, IndexStoreProvider> providers;
            {
                Map<DataFormat, IndexStoreProvider> tempProviders = new HashMap<>();
                tempProviders.put(primaryEngine.getDataFormat(), Objects.requireNonNull(primaryEngine.getProvider()));
                tempProviders.putAll(
                    secondaryEngines.stream()
                        .map(eng -> new AbstractMap.SimpleEntry<>(eng.getDataFormat(), Objects.requireNonNull(eng.getProvider())))
                        .collect(Collectors.toMap(AbstractMap.SimpleEntry::getKey, AbstractMap.SimpleEntry::getValue))
                );
                providers = tempProviders;
            }

            @Override
            public FormatStore getStore(DataFormat dataFormat) {
                return providers.get(dataFormat).getStore(dataFormat);
            }
        };
    }

    @Override
    public Map<DataFormat, EngineReaderManager<?>> buildReaderManager(ReaderManagerConfig config) throws IOException {
        Map<DataFormat, EngineReaderManager<?>> readerManagers = new HashMap<>(
            config.registry().getReaderManager(readerManagerConfig(config, primaryEngine.getDataFormat()))
        );
        for (IndexingExecutionEngine<?, ?> engine : secondaryEngines) {
            readerManagers.putAll(config.registry().getReaderManager(readerManagerConfig(config, engine.getDataFormat())));
        }
        return Map.copyOf(readerManagers);
    }

    private ReaderManagerConfig readerManagerConfig(ReaderManagerConfig config, DataFormat toAugment) {
        return new ReaderManagerConfig(config.indexStoreProvider(), toAugment, config.registry(), config.shardPath());
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
