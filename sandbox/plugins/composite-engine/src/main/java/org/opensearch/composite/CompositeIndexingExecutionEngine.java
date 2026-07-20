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
import org.opensearch.composite.stats.CompositeShardStatsTracker;
import org.opensearch.composite.stats.CompositeStatsProvider;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.DataFormatPlugin;
import org.opensearch.index.engine.dataformat.DataFormatRegistry;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.IndexingEngineConfig;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.MergeInput;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.dataformat.Merger;
import org.opensearch.index.engine.dataformat.ReaderManagerConfig;
import org.opensearch.index.engine.dataformat.RefreshInput;
import org.opensearch.index.engine.dataformat.RefreshResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.dataformat.WriterConfig;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.commit.IndexStoreProvider;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.store.FormatChecksumStrategy;
import org.opensearch.index.store.Store;
import org.opensearch.plugin.stats.StatsRecorder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.IdentityHashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import org.jspecify.annotations.NonNull;

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
    private final IndexSettings indexSettings;
    private final CompositeMerger merger;
    private final CompositeShardStatsTracker statsTracker = new CompositeShardStatsTracker();
    private final ShardId shardId;
    private volatile Map<String, Collection<String>> pendingDeletes = new ConcurrentHashMap<>();

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
        this.primaryEngine = dataFormatRegistry.getIndexingEngine(engineSettings.childConfigFor(indexSettings), primaryFormat);
        allFormats.add(primaryFormat);

        List<IndexingExecutionEngine<?, ?>> secondaries = new ArrayList<>();
        for (String secondaryName : secondaryFormatNames) {
            DataFormat secondaryFormat = dataFormatRegistry.format(secondaryName);
            secondaries.add(dataFormatRegistry.getIndexingEngine(engineSettings.childConfigFor(indexSettings), secondaryFormat));
            allFormats.add(secondaryFormat);
        }
        this.secondaryEngines = Set.copyOf(secondaries);


        this.compositeDataFormat = new CompositeDataFormat(primaryFormat, allFormats);
        this.committer = committer;
        this.indexSettings = indexSettings;
        this.merger = new CompositeMerger(this, compositeDataFormat);
        this.shardId = store != null ? store.shardId() : null;

        // Register the per-shard tracker so REST endpoints can read live counters; unregistered
        // in close(). Rolls back the registration if anything below throws, to avoid leaking it.
        CompositeStatsProvider provider = CompositeStatsProvider.getInstance();
        boolean registered = false;
        try {
            if (provider != null && shardId != null) {
                provider.register(shardId, statsTracker);
                registered = true;
            }
        } catch (Throwable t) {
            if (registered) {
                try {
                    provider.unregister(shardId);
                } catch (Throwable rollbackErr) {
                    logger.warn("Failed to unregister composite stats tracker during constructor rollback", rollbackErr);
                }
            }
            throw t;
        }
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
     * @param config the writer configuration
     * @return a composite writer bound to this engine
     */
    @Override
    public Writer<CompositeDocumentInput> createWriter(WriterConfig config) {
        return new CompositeWriter(this, config);
    }

    /** {@inheritDoc} Delegates to the primary engine's merger. */
    @Override
    public Merger getMerger() {
        return merger;
    }

    /**
     * Multiplexes tragic-exception detection across primary and secondary engines.
     * Returns the first non-null tragic exception found (primary checked first), or
     * {@code null} if every delegate is healthy.
     */
    @Override
    public Exception getTragicException() {
        Exception primaryTragic = primaryEngine.getTragicException();
        if (primaryTragic != null) return primaryTragic;
        for (IndexingExecutionEngine<?, ?> engine : secondaryEngines) {
            Exception tragic = engine.getTragicException();
            if (tragic != null) return tragic;
        }
        return null;
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
        // recordTimeMillis owns the whole-refresh timing; incRefreshTotal counts every refresh.
        statsTracker.incRefreshTotal();
        return StatsRecorder.recordTimeMillis(() -> doRefresh(refreshInput), statsTracker::addRefreshTimeMillis);
    }

    private RefreshResult doRefresh(RefreshInput refreshInput) throws IOException {
        tryDeletePendingFiles();

        // All per-format engines refresh normally (primary passes through, secondary does addIndexes)
        RefreshInput perFormatInput = new RefreshInput(refreshInput.existingSegments(), refreshInput.writerFiles());
        RefreshResult primary = primaryEngine.refresh(perFormatInput);
        List<RefreshResult> secResults = new ArrayList<>();
        for (IndexingExecutionEngine<?, ?> engine : secondaryEngines) {
            secResults.add(engine.refresh(perFormatInput));
        }

        // Assemble per-gen segments from all formats
        Map<DataFormat, RefreshResult> resultsByFormat = new LinkedHashMap<>();
        resultsByFormat.put(primaryEngine.getDataFormat(), primary);
        int i = 0;
        for (IndexingExecutionEngine<?, ?> engine : secondaryEngines) {
            resultsByFormat.put(engine.getDataFormat(), secResults.get(i++));
        }
        Map<Long, Segment.Builder> mergedByGen = new LinkedHashMap<>();
        for (Map.Entry<DataFormat, RefreshResult> entry : resultsByFormat.entrySet()) {
            buildSegment(entry.getKey(), entry.getValue(), mergedByGen);
        }

        List<Segment> newSegments = new ArrayList<>(mergedByGen.size());
        for (Segment.Builder builder : mergedByGen.values()) {
            newSegments.add(builder.build());
        }

        // Merge on refresh: when multiple writer segments exist and size is within threshold,
        // merge all formats. On failure, fall back to normal per-writer segments (background
        // merge will consolidate later). This ensures refresh never fails due to merge errors
        // (e.g., merge pool rejection, resource contention).
        if (refreshInput.hasNextGeneration() && shouldMergeOnRefresh(refreshInput.writerFiles())) {
            Set<Long> existingGens = refreshInput.existingSegments().stream().map(Segment::generation).collect(Collectors.toSet());
            List<Segment> onlyNew = newSegments.stream().filter(s -> existingGens.contains(s.generation()) == false).toList();

            if (onlyNew.size() > 1) {
                try {
                    final long mergeStartNanos = System.nanoTime();
                    // Counts merge-on-refresh attempts; a subset overlay of merge_total (also
                    // incremented inside CompositeMerger.merge()).
                    statsTracker.incRefreshMergeTotal();
                    MergeResult mergeResult = StatsRecorder.recordTimeMillis(
                        () -> merger.merge(
                            MergeInput.builder().segments(onlyNew).newWriterGeneration(refreshInput.nextAvailableGeneration()).build()
                        ),
                        statsTracker::addRefreshMergeTimeMillis
                    );

                    if (mergeResult != null) {
                        List<Segment> result = new ArrayList<>(refreshInput.existingSegments());
                        Segment mergedSegment = new Segment(
                            refreshInput.nextAvailableGeneration(),
                            mergeResult.getMergedWriterFileSet()
                                .entrySet()
                                .stream()
                                .collect(Collectors.toMap(e -> e.getKey().name(), Map.Entry::getValue))
                        );
                        result.add(mergedSegment);

                        if (logger.isDebugEnabled()) {
                            final long totalElapsedMs = java.util.concurrent.TimeUnit.NANOSECONDS.toMillis(
                                System.nanoTime() - mergeStartNanos
                            );
                            logger.debug(
                                "merge-on-refresh: merged {} segments into gen={}, total={}ms, " + "resultSegments={}, existingSegments={}",
                                onlyNew.size(),
                                refreshInput.nextAvailableGeneration(),
                                totalElapsedMs,
                                result.size(),
                                refreshInput.existingSegments().size()
                            );
                        }

                        assert result.size() == refreshInput.existingSegments().size() + 1
                            : "merge on refresh must produce exactly 1 new segment, got "
                                + (result.size() - refreshInput.existingSegments().size())
                                + " new segments";
                        assert result.stream().allMatch(s -> s.dfGroupedSearchableFiles().size() >= 1 + secondaryEngines.size())
                            : "refresh result segments must contain all configured formats";

                        for (Map.Entry<String, Collection<String>> pendingDeletionPerFormat : deleteFiles(getFilesToDelete(onlyNew))
                            .entrySet()) {
                            pendingDeletes.computeIfAbsent(pendingDeletionPerFormat.getKey(), k -> new ArrayList<>())
                                .addAll(pendingDeletionPerFormat.getValue());
                        }

                        return new RefreshResult(List.copyOf(result));
                    }
                } catch (Exception e) {
                    // Merge-on-refresh is best-effort. On failure, fall back to normal per-writer
                    // segments. Background merge will consolidate them later.
                    statsTracker.incRefreshMergeFailures();
                    logger.warn("merge-on-refresh failed, falling back to per-writer segments", e);
                }
            }
        }

        // No merge on refresh — pass through all segments
        assert newSegments.stream().allMatch(s -> s.dfGroupedSearchableFiles().size() >= 1 + secondaryEngines.size())
            : "refresh result segments must contain all configured formats";
        return new RefreshResult(List.copyOf(newSegments));
    }

    private static @NonNull Map<String, Collection<String>> getFilesToDelete(List<Segment> segmentsToPurge) {
        Map<String, Set<String>> filesToDelete = new HashMap<>();
        for (Segment segment : segmentsToPurge) {
            for (Map.Entry<String, WriterFileSet> entry : segment.dfGroupedSearchableFiles().entrySet()) {
                filesToDelete.compute(entry.getKey(), (k, v) -> {
                    Set<String> files = v;
                    if (v == null) {
                        files = new HashSet<>();
                    }
                    files.addAll(entry.getValue().files());
                    return files;
                });
            }
        }
        Map<String, Collection<String>> unmodifiable = new HashMap<>();
        for (Map.Entry<String, Set<String>> entry : filesToDelete.entrySet()) {
            unmodifiable.put(entry.getKey(), Collections.unmodifiableSet(entry.getValue()));
        }
        return unmodifiable;
    }

    private void tryDeletePendingFiles() throws IOException {
        pendingDeletes = deleteFiles(pendingDeletes);
    }

    private boolean shouldMergeOnRefresh(List<Segment> writerFiles) {
        long maxBytes = CompositeDataFormatPlugin.MERGE_ON_REFRESH_MAX_SIZE.get(indexSettings.getSettings()).getBytes();
        if (maxBytes <= 0) {
            return false;
        }
        long totalBytes = writerFiles.stream()
            .flatMap(seg -> seg.dfGroupedSearchableFiles().values().stream())
            .mapToLong(WriterFileSet::getTotalSize)
            .sum();
        return totalBytes <= maxBytes;
    }

    /**
     * Adds only the {@code ownFormat}'s {@link WriterFileSet} from each segment in
     * {@code result.refreshedSegments()} into {@code mergedByGen}. Any other formats present
     * in the per-engine result (e.g. echoed back from {@link RefreshInput#writerFiles()}) are
     * intentionally ignored so each format's authoritative entry comes solely from its own engine.
     */
    private void buildSegment(DataFormat ownFormat, RefreshResult result, Map<Long, Segment.Builder> mergedByGen) {
        for (Segment seg : result.refreshedSegments()) {
            WriterFileSet ownFiles = seg.dfGroupedSearchableFiles().get(ownFormat.name());
            Segment.Builder builder = mergedByGen.computeIfAbsent(seg.generation(), Segment::builder);
            builder.addSearchableFiles(ownFormat, ownFiles);
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

    @Override
    public long getHeapBytesUsed() {
        long total = primaryEngine.getHeapBytesUsed();
        for (IndexingExecutionEngine<?, ?> engine : secondaryEngines) {
            total += engine.getHeapBytesUsed();
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
        CompositeStatsProvider provider = CompositeStatsProvider.getInstance();
        if (provider != null && shardId != null) {
            provider.unregister(shardId);
        }
        IOUtils.closeWhileHandlingException(primaryEngine);
        secondaryEngines.forEach(IOUtils::closeWhileHandlingException);
        IOUtils.closeWhileHandlingException(committer);
    }

    /** Returns this shard's composite stats tracker, used by the writer and merger to count. */
    public CompositeShardStatsTracker statsTracker() {
        return statsTracker;
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
                tempProviders.put(primaryEngine.getDataFormat(), primaryEngine.getProvider());
                if (primaryEngine.getProvider() == null) {
                    logger.debug("IndexStoreProvider is null for primary engine [{}]", primaryEngine.getDataFormat().name());
                }
                for (IndexingExecutionEngine<?, ?> eng : secondaryEngines) {
                    tempProviders.put(eng.getDataFormat(), eng.getProvider());
                    if (eng.getProvider() == null) {
                        logger.debug("IndexStoreProvider is null for secondary engine [{}]", eng.getDataFormat().name());
                    }
                }
                providers = tempProviders;
            }

            @Override
            public FormatStore getStore(DataFormat dataFormat) {
                IndexStoreProvider provider = providers.get(dataFormat);
                return provider != null ? provider.getStore(dataFormat) : null;
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
        return new ReaderManagerConfig(
            config.indexStoreProvider(),
            toAugment,
            config.registry(),
            config.shardPath(),
            config.dataformatAwareStoreHandles(),
            config.indexSettings()
        );
    }

    /**
     * Returns the secondary delegate engines.
     *
     * @return the secondary engines
     */
    public Set<IndexingExecutionEngine<?, ?>> getSecondaryDelegates() {
        return secondaryEngines;
    }

    @Override
    public long maxIndexableDocs() {
        long maxAllowedDocs = primaryEngine.maxIndexableDocs();
        for (var engine : secondaryEngines) {
            maxAllowedDocs = Math.min(maxAllowedDocs, engine.maxIndexableDocs());
        }
        return maxAllowedDocs;
    }
}
