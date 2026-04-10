/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite.merge;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.logging.Loggers;
import org.opensearch.composite.CompositeDataFormat;
import org.opensearch.composite.CompositeIndexingExecutionEngine;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.MergeInput;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.dataformat.Merger;
import org.opensearch.index.engine.dataformat.RowIdMapping;
import org.opensearch.index.engine.dataformat.merge.DataFormatAwareMergePolicy;
import org.opensearch.index.engine.dataformat.merge.MergeHandler;
import org.opensearch.index.engine.dataformat.merge.OneMerge;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * {@link MergeHandler} implementation for composite data formats.
 * <p>
 * Implements {@link #doMerge(OneMerge)} to merge the primary data format first,
 * then merge each secondary using the row-ID mapping produced by the primary.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CompositeMergeHandler extends MergeHandler {

    private final CompositeIndexingExecutionEngine compositeIndexingExecutionEngine;
    private final DataFormat primaryDataFormat;
    private final Map<DataFormat, Merger> dataFormatMergerMap;
    private final Logger logger;

    /**
     * Constructs a CompositeMergeHandler.
     *
     * @param compositeIndexingExecutionEngine the composite engine providing primary and secondary delegates
     * @param compositeDataFormat              the composite data format with primary format reference
     * @param snapshotSupplier                 supplier for acquiring catalog snapshots
     * @param indexSettings                    the index settings containing merge policy configuration
     * @param shardId                          the shard ID for logging context
     */
    public CompositeMergeHandler(
        CompositeIndexingExecutionEngine compositeIndexingExecutionEngine,
        CompositeDataFormat compositeDataFormat,
        Supplier<GatedCloseable<CatalogSnapshot>> snapshotSupplier,
        IndexSettings indexSettings,
        ShardId shardId
    ) {
        super(snapshotSupplier, new DataFormatAwareMergePolicy(indexSettings.getMergePolicy(true), shardId), shardId);
        this.logger = Loggers.getLogger(getClass(), shardId);
        this.compositeIndexingExecutionEngine = compositeIndexingExecutionEngine;
        this.primaryDataFormat = compositeDataFormat.getPrimaryDataFormat();
        this.dataFormatMergerMap = buildMergerMap(compositeIndexingExecutionEngine);
    }

    @Override
    public MergeResult doMerge(OneMerge oneMerge) {
        long mergedWriterGeneration = compositeIndexingExecutionEngine.getNextWriterGeneration();
        Map<DataFormat, WriterFileSet> mergedWriterFileSet = new HashMap<>();
        boolean mergeSuccessful = false;
        try {
            List<WriterFileSet> primaryFiles = getFilesToMerge(oneMerge, primaryDataFormat);
            MergeResult primaryMergeResult = dataFormatMergerMap.get(primaryDataFormat)
                .merge(new MergeInput(primaryFiles, null, mergedWriterGeneration));

            mergedWriterFileSet.put(primaryDataFormat, primaryMergeResult.getMergedWriterFileSetForDataformat(primaryDataFormat));

            boolean hasSecondaries = compositeIndexingExecutionEngine.getSecondaryDelegates()
                .stream()
                .anyMatch(engine -> engine.getDataFormat().equals(primaryDataFormat) == false);

            RowIdMapping rowIdMapping = null;
            if (hasSecondaries) {
                rowIdMapping = primaryMergeResult.rowIdMapping()
                    .orElseThrow(
                        () -> new IllegalStateException("Primary merge did not produce a row-ID mapping required by secondary formats")
                    );
            }

            for (IndexingExecutionEngine<?, ?> secondaryEngine : compositeIndexingExecutionEngine.getSecondaryDelegates()) {
                DataFormat secondaryDataFormat = secondaryEngine.getDataFormat();
                if (secondaryDataFormat.equals(primaryDataFormat)) {
                    continue;
                }

                List<WriterFileSet> secondaryFiles = getFilesToMerge(oneMerge, secondaryDataFormat);
                MergeResult secondaryMergeResult = dataFormatMergerMap.get(secondaryDataFormat)
                    .merge(new MergeInput(secondaryFiles, rowIdMapping, mergedWriterGeneration));

                mergedWriterFileSet.put(secondaryDataFormat, secondaryMergeResult.getMergedWriterFileSetForDataformat(secondaryDataFormat));
            }

            mergeSuccessful = true;
            return new MergeResult(mergedWriterFileSet, rowIdMapping);
        } catch (IOException e) {
            throw new UncheckedIOException("Merge failed for shard", e);
        } finally {
            if (mergeSuccessful == false && mergedWriterFileSet.isEmpty() == false) {
                cleanupStaleMergedFiles(mergedWriterFileSet);
            }
        }
    }

    private List<WriterFileSet> getFilesToMerge(OneMerge oneMerge, DataFormat dataFormat) {
        List<WriterFileSet> writerFileSets = new ArrayList<>();
        for (Segment segment : oneMerge.getSegmentsToMerge()) {
            writerFileSets.add(segment.dfGroupedSearchableFiles().get(dataFormat.name()));
        }
        return writerFileSets;
    }

    private void cleanupStaleMergedFiles(Map<DataFormat, WriterFileSet> mergedWriterFileSet) {
        for (WriterFileSet wfs : mergedWriterFileSet.values()) {
            for (String file : wfs.files()) {
                Path path = Path.of(wfs.directory(), file);
                try {
                    Files.deleteIfExists(path);
                } catch (Exception exception) {
                    logger.error(new ParameterizedMessage("Failed to delete stale merged file [{}]", path), exception);
                }
            }
        }
    }

    private static Map<DataFormat, Merger> buildMergerMap(CompositeIndexingExecutionEngine compositeEngine) {
        Map<DataFormat, Merger> map = new HashMap<>();
        map.put(compositeEngine.getPrimaryDelegate().getDataFormat(), compositeEngine.getPrimaryDelegate().getMerger());
        compositeEngine.getSecondaryDelegates().forEach(engine -> map.put(engine.getDataFormat(), engine.getMerger()));
        return Map.copyOf(map);
    }
}
