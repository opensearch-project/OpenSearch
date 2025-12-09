/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.merge;

import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.exec.composite.CompositeIndexingExecutionEngine;
import org.opensearch.index.engine.exec.coord.Any;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.engine.exec.coord.CompositeEngine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

public class CompositeMergeHandler extends MergeHandler {

    private final CompositeMergePolicy mergePolicy;
    private final CompositeEngine compositeEngine;
    private final CompositeIndexingExecutionEngine compositeIndexingExecutionEngine;

    public CompositeMergeHandler(
        CompositeEngine compositeEngine,
        CompositeIndexingExecutionEngine compositeIndexingExecutionEngine,
        Any dataFormats,
        IndexSettings indexSettings
    ) {
        super(compositeEngine, compositeIndexingExecutionEngine, dataFormats);
        this.compositeEngine = compositeEngine;
        this.compositeIndexingExecutionEngine = compositeIndexingExecutionEngine;

        mergePolicy = new CompositeMergePolicy(indexSettings.getMergePolicy(true));
    }

    @Override
    public Collection<OneMerge> findForceMerges(int maxSegmentCount) {
        List<OneMerge> oneMerges = new ArrayList<>();
        try (CompositeEngine.ReleasableRef<CatalogSnapshot> catalogSnapshotReleasableRef = compositeEngine.acquireSnapshot()) {
            CatalogSnapshot catalogSnapshot = catalogSnapshotReleasableRef.getRef();

            List<CatalogSnapshot.Segment> segmentList = catalogSnapshot.getSegments();
            List<List<CatalogSnapshot.Segment>> mergeCandidates =
                mergePolicy.findForceMergeCandidates(segmentList, maxSegmentCount);

            // Process merge candidates
            for (List<CatalogSnapshot.Segment> mergeGroup : mergeCandidates) {
                oneMerges.add(new OneMerge(mergeGroup));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return oneMerges;
    }

    @Override
    public Collection<OneMerge> findMerges() {
        List<OneMerge> oneMerges = new ArrayList<>();
        try (CompositeEngine.ReleasableRef<CatalogSnapshot> catalogSnapshotReleasableRef = compositeEngine.acquireSnapshot()) {
            CatalogSnapshot catalogSnapshot = catalogSnapshotReleasableRef.getRef();

            List<CatalogSnapshot.Segment> segmentList = catalogSnapshot.getSegments();
            List<List<CatalogSnapshot.Segment>> mergeCandidates =
                mergePolicy.findMergeCandidates(segmentList);

            // Process merge candidates
            for (List<CatalogSnapshot.Segment> mergeGroup : mergeCandidates) {
                oneMerges.add(new OneMerge(mergeGroup));
            }
        } catch (Exception e) {
            e.printStackTrace();
            throw new RuntimeException(e);
        }
        return oneMerges;
    }

    public synchronized void registerMerge(OneMerge oneMerge) {
        super.registerMerge(oneMerge);
        mergePolicy.addMergingSegment(oneMerge.getSegmentsToMerge());
    }

    public synchronized void onMergeFinished(OneMerge oneMerge) {
        super.onMergeFinished(oneMerge);
        mergePolicy.removeMergingSegment(oneMerge.getSegmentsToMerge());
    }

    public synchronized void onMergeFailure(OneMerge oneMerge) {
        super.onMergeFailure(oneMerge);
        mergePolicy.removeMergingSegment(oneMerge.getSegmentsToMerge());
    }
}
