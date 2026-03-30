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
import org.opensearch.ExceptionsHelper;
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
import org.opensearch.index.engine.dataformat.merge.MergeHandler;
import org.opensearch.index.engine.dataformat.merge.OneMerge;
import org.opensearch.index.engine.exec.Indexer;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link MergeHandler} implementation for composite data formats.
 * <p>
 * Delegates merge candidate selection to a {@link CompositeMergePolicy} backed by the
 * configured Lucene {@link org.apache.lucene.index.MergePolicy}, then executes merges
 * per data format — primary first, secondaries with the row-ID mapping produced by the
 * primary merge.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class CompositeMergeHandler extends MergeHandler {

    private final CompositeMergePolicy mergePolicy;
    private final CompositeIndexingExecutionEngine compositeIndexingExecutionEngine;
    private final CompositeDataFormat compositeDataFormat;
    private final Indexer indexer;
    private final Map<DataFormat, Merger> dataFormatMergerMap;
    private final Logger logger;

    /**
     * Constructs a CompositeMergeHandler.
     *
     * @param compositeIndexingExecutionEngine the composite engine providing primary and secondary delegates
     * @param compositeDataFormat              the composite data format with primary format reference
     * @param indexer                          the indexer for acquiring catalog snapshots
     * @param indexSettings                    the index settings containing merge policy configuration
     * @param shardId                          the shard ID for logging context
     */
    public CompositeMergeHandler(
        CompositeIndexingExecutionEngine compositeIndexingExecutionEngine,
        CompositeDataFormat compositeDataFormat,
        Indexer indexer,
        IndexSettings indexSettings,
        ShardId shardId
    ) {
        super(indexer, shardId);
        this.logger = Loggers.getLogger(getClass(), shardId);
        this.mergePolicy = new CompositeMergePolicy(indexSettings.getMergePolicy(true), shardId);
        this.compositeIndexingExecutionEngine = compositeIndexingExecutionEngine;
        this.compositeDataFormat = compositeDataFormat;
        this.indexer = indexer;
        this.dataFormatMergerMap = new HashMap<>();

        dataFormatMergerMap.put(
            compositeIndexingExecutionEngine.getPrimaryDelegate().getDataFormat(),
            compositeIndexingExecutionEngine.getPrimaryDelegate().getMerger()
        );
        compositeIndexingExecutionEngine.getSecondaryDelegates().forEach(engine -> {
            dataFormatMergerMap.put(engine.getDataFormat(), engine.getMerger());
        });
    }

    /** {@inheritDoc} */
    @Override
    public Collection<OneMerge> findMerges() {
        List<OneMerge> oneMerges = new ArrayList<>();
        try (GatedCloseable<CatalogSnapshot> catalogSnapshotReleasableRef = indexer.acquireSnapshot()) {
            CatalogSnapshot catalogSnapshot = catalogSnapshotReleasableRef.get();

            List<Segment> segmentList = catalogSnapshot.getSegments();
            List<List<Segment>> mergeCandidates = mergePolicy.findMergeCandidates(segmentList);

            // Process merge candidates
            for (List<Segment> mergeGroup : mergeCandidates) {
                oneMerges.add(new OneMerge(mergeGroup));
            }
        } catch (Exception e) {
            logger.warn("Failed to acquire snapshots", e);
            throw new RuntimeException(e);
        }
        return oneMerges;
    }

    /**
     * {@inheritDoc}
     *
     * @param maxSegmentCount the maximum number of segments allowed after merging
     */
    @Override
    public Collection<OneMerge> findForceMerges(int maxSegmentCount) {
        List<OneMerge> oneMerges = new ArrayList<>();
        try (GatedCloseable<CatalogSnapshot> catalogSnapshotReleasableRef = indexer.acquireSnapshot()) {
            CatalogSnapshot catalogSnapshot = catalogSnapshotReleasableRef.get();

            List<Segment> segmentList = catalogSnapshot.getSegments();
            List<List<Segment>> mergeCandidates = mergePolicy.findForceMergeCandidates(segmentList, maxSegmentCount);

            // Process merge candidates
            for (List<Segment> mergeGroup : mergeCandidates) {
                oneMerges.add(new OneMerge(mergeGroup));
            }
        } catch (Exception e) {
            logger.warn("Failed to acquire snapshots", e);
            throw new RuntimeException(e);
        }
        return oneMerges;
    }

    /**
     * {@inheritDoc}
     *
     * Executes the merge per data format — primary first, then secondaries
     * using the row-ID mapping produced by the primary merge.
     *
     * @param oneMerge the merge to execute
     */
    @Override
    public MergeResult doMerge(OneMerge oneMerge) {
        long mergedWriterGeneration = compositeIndexingExecutionEngine.getNextWriterGeneration();
        Map<DataFormat, WriterFileSet> mergedWriterFileSet = new HashMap<>();
        boolean mergeSuccessful = false;
        DataFormat primaryFormat = compositeDataFormat.getPrimaryDataFormat();

        try {
            List<WriterFileSet> filesToMerge = getFilesToMerge(oneMerge, primaryFormat);

            MergeInput mergeInput = MergeInput.builder().fileMetadataList(filesToMerge).newWriterGeneration(mergedWriterGeneration).build();

            // Merging primary data format
            MergeResult primaryMergeResult = dataFormatMergerMap.get(primaryFormat).merge(mergeInput);
            mergedWriterFileSet.put(primaryFormat, primaryMergeResult.getMergedWriterFileSetForDataformat(primaryFormat));

            // Merge secondary formats using the row-ID mapping produced by the primary merge.
            IOException mergeException = null;
            for (IndexingExecutionEngine<?, ?> secondaryEngine : compositeIndexingExecutionEngine.getSecondaryDelegates()) {
                DataFormat df = secondaryEngine.getDataFormat();
                if (df.equals(primaryFormat)) {
                    continue;
                }
                try {
                    List<WriterFileSet> files = getFilesToMerge(oneMerge, df);

                    MergeInput secondaryMergeInput = MergeInput.builder()
                        .fileMetadataList(files)
                        .newWriterGeneration(mergedWriterGeneration)
                        .rowIdMapping(
                            primaryMergeResult.rowIdMapping()
                                .orElseThrow(
                                    () -> new IllegalStateException(
                                        "Primary merge did not produce a row-ID mapping required for secondary format [" + df.name() + "]"
                                    )
                                )
                        )
                        .build();

                    MergeResult secondaryMerge = dataFormatMergerMap.get(df).merge(secondaryMergeInput);
                    mergedWriterFileSet.put(df, secondaryMerge.getMergedWriterFileSetForDataformat(df));
                } catch (IOException e) {
                    logger.error(new ParameterizedMessage("Failed to merge secondary format [{}]", df.name()), e);
                    mergeException = ExceptionsHelper.useOrSuppress(mergeException, e);
                }
            }
            if (mergeException != null) {
                throw mergeException;
            }

            MergeResult mergeResult = new MergeResult(mergedWriterFileSet);
            mergeSuccessful = true;
            return mergeResult;

        } catch (IOException e) {
            throw new UncheckedIOException("Merge failed for shard", e);
        } finally {
            if (!mergeSuccessful && !mergedWriterFileSet.isEmpty()) {
                cleanupStaleMergedFiles(mergedWriterFileSet);
            }
        }
    }

    /**
     * {@inheritDoc}
     *
     * @param oneMerge the merge to register
     */
    @Override
    public synchronized void registerMerge(OneMerge oneMerge) {
        super.registerMerge(oneMerge);
        mergePolicy.addMergingSegment(oneMerge.getSegmentsToMerge());
    }

    /**
     * {@inheritDoc}
     *
     * @param oneMerge the merge that finished
     */
    @Override
    public synchronized void onMergeFinished(OneMerge oneMerge) {
        super.onMergeFinished(oneMerge);
        mergePolicy.removeMergingSegment(oneMerge.getSegmentsToMerge());
    }

    /**
     * {@inheritDoc}
     *
     * @param oneMerge the merge that failed
     */
    @Override
    public synchronized void onMergeFailure(OneMerge oneMerge) {
        super.onMergeFailure(oneMerge);
        mergePolicy.removeMergingSegment(oneMerge.getSegmentsToMerge());
    }

    /**
     * Collects the {@link WriterFileSet} entries for the given data format from each segment in the merge.
     *
     * @param oneMerge   the merge whose segments to inspect
     * @param dataFormat the data format to extract files for
     * @return the list of per-segment file sets for the given format
     */
    private List<WriterFileSet> getFilesToMerge(OneMerge oneMerge, DataFormat dataFormat) {
        List<WriterFileSet> writerFileSets = new ArrayList<>();
        for (Segment segment : oneMerge.getSegmentsToMerge()) {
            writerFileSets.add(segment.dfGroupedSearchableFiles().get(dataFormat.name()));
        }
        return writerFileSets;
    }

    /**
     * Best-effort cleanup of files produced by a partially-completed merge.
     *
     * @param mergedWriterFileSet the per-format file sets to delete
     */
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
}
