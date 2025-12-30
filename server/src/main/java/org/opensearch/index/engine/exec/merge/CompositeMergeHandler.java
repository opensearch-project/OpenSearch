/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.merge;

import org.opensearch.index.engine.exec.coord.Segment;

import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.opensearch.common.logging.Loggers;
import org.opensearch.core.index.shard.ShardId;
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
    private final Logger logger;

    public CompositeMergeHandler(
        CompositeEngine compositeEngine,
        CompositeIndexingExecutionEngine compositeIndexingExecutionEngine,
        Any dataFormats,
        IndexSettings indexSettings,
        ShardId shardId
    ) {
        super(compositeEngine, compositeIndexingExecutionEngine, dataFormats, shardId);
        this.logger = Loggers.getLogger(getClass(), shardId);
        this.compositeEngine = compositeEngine;
        this.compositeIndexingExecutionEngine = compositeIndexingExecutionEngine;

        mergePolicy = new CompositeMergePolicy(indexSettings.getMergePolicy(true), shardId);
    }

    @Override
    public Collection<OneMerge> findForceMerges(int maxSegmentCount) {
        List<OneMerge> oneMerges = new ArrayList<>();
        try (CompositeEngine.ReleasableRef<CatalogSnapshot> catalogSnapshotReleasableRef = compositeEngine.acquireSnapshot()) {
            CatalogSnapshot catalogSnapshot = catalogSnapshotReleasableRef.getRef();

            List<Segment> segmentList = catalogSnapshot.getSegments();
            List<List<Segment>> mergeCandidates =
                mergePolicy.findForceMergeCandidates(segmentList, maxSegmentCount);

            // Process merge candidates
            for (List<Segment> mergeGroup : mergeCandidates) {
                oneMerges.add(new OneMerge(mergeGroup));
            }
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("Failed to acquire snapshots", e));
            throw new RuntimeException(e);
        }
        return oneMerges;
    }

    @Override
    public Collection<OneMerge> findMerges() {
        List<OneMerge> oneMerges = new ArrayList<>();
        try (CompositeEngine.ReleasableRef<CatalogSnapshot> catalogSnapshotReleasableRef = compositeEngine.acquireSnapshot()) {
            CatalogSnapshot catalogSnapshot = catalogSnapshotReleasableRef.getRef();

            List<Segment> segmentList = catalogSnapshot.getSegments();
            List<List<Segment>> mergeCandidates =
                mergePolicy.findMergeCandidates(segmentList);

            // Process merge candidates
            for (List<Segment> mergeGroup : mergeCandidates) {
                oneMerges.add(new OneMerge(mergeGroup));
            }
        } catch (Exception e) {
            logger.warn(() -> new ParameterizedMessage("Failed to acquire snapshots", e));
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
