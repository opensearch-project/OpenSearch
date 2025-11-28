/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.merge;

import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.Merger;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.composite.CompositeIndexingExecutionEngine;
import org.opensearch.index.engine.exec.coord.Any;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CompositeEngine;

import java.util.ArrayDeque;
import java.util.ArrayList;
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

    public MergeHandler(CompositeEngine compositeEngine, CompositeIndexingExecutionEngine compositeIndexingExecutionEngine, Any dataFormats) {
        this.compositeDataFormat = dataFormats;
        this.compositeIndexingExecutionEngine = compositeIndexingExecutionEngine;
        this.compositeEngine = compositeEngine;
        dataFormatMergerMap = new HashMap<>();

        compositeIndexingExecutionEngine.getDelegates().forEach(engine -> {
            try {
                dataFormatMergerMap.put(engine.getDataFormat(), engine.getMerger());
            } catch (Exception e) {
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
            throw new RuntimeException(e);
        }
        mergingSegments.add(merge);
        currentlyMergingSegments.addAll(merge.getSegmentsToMerge());
    }

    public boolean hasPendingMerges() {
        return mergingSegments.size() > 0;
    }

    public synchronized OneMerge getNextMerge() {
        if(mergingSegments.isEmpty()) {
            return null;
        }
        OneMerge oneMerge = mergingSegments.removeFirst();
        return oneMerge;
    }

    public synchronized void onMergeFinished(OneMerge oneMerge) {
        removeMergingSegments(oneMerge);
        updatePendingMerges();
    }

    public synchronized void onMergeFailure(OneMerge oneMerge) {
        removeMergingSegments(oneMerge);
        System.out.println("Merge FAILED for oneMerge: " + oneMerge);
    }

    private synchronized void removeMergingSegments(OneMerge oneMerge) {
        mergingSegments.remove(oneMerge);
        currentlyMergingSegments.removeAll(oneMerge.getSegmentsToMerge());
    }

    public MergeResult doMerge(OneMerge oneMerge) {

        long mergedWriterGeneration = compositeIndexingExecutionEngine.getNextWriterGeneration();
        Map<DataFormat, WriterFileSet> mergedWriterFileSet = new HashMap<>();
        try(CompositeEngine.ReleasableRef<CatalogSnapshot> catalogSnapshot = compositeEngine.acquireSnapshot()) {

            List<WriterFileSet> filesToMerge = getFilesToMerge(oneMerge, compositeDataFormat.getPrimaryDataFormat());

            // Merging primary data format
            MergeResult primaryMergeResult = dataFormatMergerMap.get(compositeDataFormat.getPrimaryDataFormat()).merge(filesToMerge, mergedWriterGeneration);
            mergedWriterFileSet.put(compositeDataFormat.getPrimaryDataFormat(), primaryMergeResult.getMergedWriterFileSetForDataformat(compositeDataFormat.getPrimaryDataFormat()));
            // Merging other format as per the old segment + row id -> new row id mapping.
            compositeIndexingExecutionEngine.getDelegates().stream()
                    .filter(engine -> !engine.getDataFormat().equals(compositeDataFormat.getPrimaryDataFormat()))
                    .forEach(indexingExecutionEngine -> {
                        DataFormat dataFormat = indexingExecutionEngine.getDataFormat();
                        List<WriterFileSet> files = getFilesToMerge(oneMerge, dataFormat);
                        MergeResult secondaryMergeResult = dataFormatMergerMap.get(dataFormat).merge(files, primaryMergeResult.getRowIdMapping(), mergedWriterGeneration);
                        mergedWriterFileSet.put(dataFormat, secondaryMergeResult.getMergedWriterFileSetForDataformat(dataFormat));
                    });
            return new MergeResult(primaryMergeResult.getRowIdMapping(), mergedWriterFileSet);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public List<WriterFileSet> getFilesToMerge(OneMerge oneMerge, DataFormat dataFormat) {
        List<WriterFileSet> writerFileSets = new ArrayList<>();
        for (CatalogSnapshot.Segment segment : oneMerge.getSegmentsToMerge()) {
            writerFileSets.add(segment.getDFGroupedSearchableFiles().get(dataFormat.name()));
        }
        return writerFileSets;
    }
}
