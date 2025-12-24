/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.merge;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.logging.Loggers;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.MergeInput;
import org.opensearch.index.engine.exec.Merger;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.composite.CompositeIndexingExecutionEngine;
import org.opensearch.index.engine.exec.coord.Any;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CompositeEngine;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public abstract class MergeHandler {

    private final Any compositeDataFormat;

    private final CompositeIndexingExecutionEngine compositeIndexingExecutionEngine;

    private CompositeEngine compositeEngine;
    private Map<DataFormat, Merger> dataFormatMergerMap;
    private final Deque<OneMerge> mergingSegments = new ArrayDeque<>();
    private final Set<CatalogSnapshot.Segment> currentlyMergingSegments = new HashSet<>();
    private final Logger logger;
    private final String sortKey;
    private final boolean reverseSort;

    public MergeHandler(
        CompositeEngine compositeEngine,
        CompositeIndexingExecutionEngine compositeIndexingExecutionEngine,
        Any dataFormats,
        ShardId shardId
    ) {
        this.logger = Loggers.getLogger(getClass(), shardId);
        this.compositeDataFormat = dataFormats;
        this.compositeIndexingExecutionEngine = compositeIndexingExecutionEngine;
        this.compositeEngine = compositeEngine;
        dataFormatMergerMap = new HashMap<>();
        sortKey =
            compositeEngine.getEngineConfig().getIndexSort() != null &&
            compositeEngine.getEngineConfig().getIndexSort().getSort().length > 0 &&
            Arrays.stream(compositeEngine.getEngineConfig().getIndexSort().getSort()).findFirst().isPresent()
                ? Arrays.stream(compositeEngine.getEngineConfig().getIndexSort().getSort()).findFirst().get().getField()
                  : null;
        reverseSort = compositeEngine.getEngineConfig().getIndexSort() != null &&
            compositeEngine.getEngineConfig().getIndexSort().getSort().length > 0 &&
            Arrays.stream(compositeEngine.getEngineConfig().getIndexSort().getSort()).findFirst().isPresent() &&
            Arrays.stream(compositeEngine.getEngineConfig().getIndexSort().getSort()).findFirst().get().getReverse();

        compositeIndexingExecutionEngine.getDelegates().forEach(engine -> {
            try {
                dataFormatMergerMap.put(engine.getDataFormat(), engine.getMerger());
            } catch (Exception e) {
                logger.error(() -> new ParameterizedMessage("Failed to create merger for data format [{}]",
                    engine.getDataFormat(), e));
                throw new RuntimeException(e);
            }
        });
    }

    public abstract Collection<OneMerge> findMerges();

    public abstract Collection<OneMerge> findForceMerges(int maxSegmentCount);

    public synchronized void updatePendingMerges() {
        Collection<OneMerge> oneMerges = findMerges();
        for (OneMerge oneMerge : oneMerges) {
            boolean isValidMerge = true;
            for (CatalogSnapshot.Segment segment : oneMerge.getSegmentsToMerge()) {
                if (currentlyMergingSegments.contains(segment)) {
                    isValidMerge = false;
                    break;
                }
            }
            if (isValidMerge) {
                registerMerge(oneMerge);
            }
        }
    }

    public synchronized void registerMerge(OneMerge merge) {
        try (CompositeEngine.ReleasableRef<CatalogSnapshot> catalogSnapshotReleasableRef = compositeEngine.acquireSnapshot()) {
            // Validate segments exist in catalog
            List<CatalogSnapshot.Segment> catalogSegments = catalogSnapshotReleasableRef.getRef().getSegments();
            for (CatalogSnapshot.Segment mergeSegment : merge.getSegmentsToMerge()) {
                if (!catalogSegments.contains(mergeSegment)) {
                    return;
                }
            }
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("Failed to acquire snapshots", e));
            throw new RuntimeException(e);
        }
        mergingSegments.add(merge);
        currentlyMergingSegments.addAll(merge.getSegmentsToMerge());
        logger.debug(() -> new ParameterizedMessage("Registered merge [{}], mergingSegments: [{}]",
            merge, mergingSegments));
    }

    public boolean hasPendingMerges() {
        return !mergingSegments.isEmpty();
    }

    public synchronized OneMerge getNextMerge() {
        if(mergingSegments.isEmpty()) {
            return null;
        }
        return mergingSegments.removeFirst();
    }

    public synchronized void onMergeFinished(OneMerge oneMerge) {
        removeMergingSegments(oneMerge);
        updatePendingMerges();
    }

    public synchronized void onMergeFailure(OneMerge oneMerge) {
        removeMergingSegments(oneMerge);
        logger.warn(() -> new ParameterizedMessage("Merge failed for OneMerge [{}]", oneMerge));
    }

    private synchronized void removeMergingSegments(OneMerge oneMerge) {
        mergingSegments.remove(oneMerge);
        oneMerge.getSegmentsToMerge().forEach(currentlyMergingSegments::remove);
    }

    public MergeResult doMerge(OneMerge oneMerge) {

        long mergedWriterGeneration = compositeIndexingExecutionEngine.getNextWriterGeneration();
        Map<DataFormat, WriterFileSet> mergedWriterFileSet = new HashMap<>();
        List<WriterFileSet> filesToMerge = getFilesToMerge(oneMerge, compositeDataFormat.getPrimaryDataFormat());

        // Merging primary data format
        MergeResult primaryMergeResult = dataFormatMergerMap.get(compositeDataFormat.getPrimaryDataFormat())
            .merge(getMergeInput(filesToMerge, mergedWriterGeneration, sortKey, reverseSort));
        mergedWriterFileSet.put(compositeDataFormat.getPrimaryDataFormat(), primaryMergeResult.getMergedWriterFileSetForDataformat(compositeDataFormat.getPrimaryDataFormat()));
        // Merging other format as per the old segment + row id -> new row id mapping.
        compositeIndexingExecutionEngine.getDelegates().stream()
                .filter(engine -> !engine.getDataFormat().equals(compositeDataFormat.getPrimaryDataFormat()))
                .forEach(indexingExecutionEngine -> {
                    DataFormat dataFormat = indexingExecutionEngine.getDataFormat();
                    List<WriterFileSet> files = getFilesToMerge(oneMerge, dataFormat);
                    MergeResult secondaryMergeResult = dataFormatMergerMap.get(dataFormat)
                        .merge(getMergeInput(files, mergedWriterGeneration, sortKey, reverseSort), primaryMergeResult.getRowIdMapping());
                    mergedWriterFileSet.put(dataFormat, secondaryMergeResult.getMergedWriterFileSetForDataformat(dataFormat));
                });
        return new MergeResult(primaryMergeResult.getRowIdMapping(), mergedWriterFileSet);
    }

    public MergeInput getMergeInput(List<WriterFileSet> filesToMerge, long mergedWriterGeneration, String sortKey, boolean reverseSort) {
        return new MergeInput(filesToMerge, mergedWriterGeneration, sortKey, reverseSort);
    }

    private List<WriterFileSet> getFilesToMerge(OneMerge oneMerge, DataFormat dataFormat) {
        List<WriterFileSet> writerFileSets = new ArrayList<>();
        for (CatalogSnapshot.Segment segment : oneMerge.getSegmentsToMerge()) {
            writerFileSets.add(segment.getDFGroupedSearchableFiles().get(dataFormat.name()));
        }
        return writerFileSets;
    }
}
