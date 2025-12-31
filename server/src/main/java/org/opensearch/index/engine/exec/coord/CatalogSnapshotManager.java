/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.opensearch.index.engine.exec.coord.Segment;

import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.RefreshResult;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.merge.MergeResult;
import org.opensearch.index.engine.exec.merge.OneMerge;
import org.opensearch.index.shard.ShardPath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.index.engine.exec.coord.CatalogSnapshot.CATALOG_SNAPSHOT_KEY;

public class CatalogSnapshotManager {

    private CompositeEngineCatalogSnapshot latestCatalogSnapshot;
    private final Committer compositeEngineCommitter;
    private final Map<Long, CompositeEngineCatalogSnapshot> catalogSnapshotMap;
    private final AtomicReference<IndexFileDeleter> indexFileDeleter;

    public CatalogSnapshotManager(CompositeEngine compositeEngine, Committer compositeEngineCommitter, ShardPath shardPath) throws IOException {
        catalogSnapshotMap = new HashMap<>();
        this.compositeEngineCommitter = compositeEngineCommitter;
        indexFileDeleter = new AtomicReference<>();
        getLastCommittedCatalogSnapshot().ifPresent(lastCommittedCatalogSnapshot -> {
            latestCatalogSnapshot = lastCommittedCatalogSnapshot;
            catalogSnapshotMap.put(latestCatalogSnapshot.getId(), latestCatalogSnapshot);
            latestCatalogSnapshot.remapPaths(shardPath.getDataPath());
        });
        indexFileDeleter.set(new IndexFileDeleter(compositeEngine, latestCatalogSnapshot, shardPath));
        if(latestCatalogSnapshot != null) {
            latestCatalogSnapshot.setIndexFileDeleterSupplier(indexFileDeleter::get);
            latestCatalogSnapshot.setCatalogSnapshotMap(catalogSnapshotMap);
        } else {
            latestCatalogSnapshot = new CompositeEngineCatalogSnapshot(1, 1, new ArrayList<>(), catalogSnapshotMap, indexFileDeleter::get);
            catalogSnapshotMap.put(latestCatalogSnapshot.getId(), latestCatalogSnapshot);
        }
    }

    public CompositeEngine.ReleasableRef<CatalogSnapshot> acquireSnapshot() {
        final CatalogSnapshot snapshot = latestCatalogSnapshot;
        if (snapshot != null) snapshot.incRef();
        return new CompositeEngine.ReleasableRef<>(snapshot) {
            @Override
            public void close() {
                if (snapshot != null) snapshot.decRef();
            }
        };
    }

    public synchronized void applyRefreshResult(RefreshResult refreshResult) {
        commitCatalogSnapshot(
            new CompositeEngineCatalogSnapshot(
                latestCatalogSnapshot.getId() + 1,
                latestCatalogSnapshot.getVersion() + 1,
                refreshResult.getRefreshedSegments(),
                catalogSnapshotMap,
                indexFileDeleter::get)
        );
    }

    public synchronized void applyReplicationChanges(CatalogSnapshot catalogSnapshot, ShardPath shardPath) {
        CompositeEngineCatalogSnapshot oldSnapshot = latestCatalogSnapshot;
        if (catalogSnapshot != null) {
            catalogSnapshot.incRef();
            catalogSnapshot.remapPaths(shardPath.getDataPath());
            latestCatalogSnapshot = (CompositeEngineCatalogSnapshot) catalogSnapshot;
            catalogSnapshotMap.put(latestCatalogSnapshot.getId(), latestCatalogSnapshot);
        }
        if (oldSnapshot != null) {
            oldSnapshot.decRef();
        }
    }

    public synchronized void applyMergeResults(MergeResult mergeResult, OneMerge oneMerge) {

        List<Segment> segmentList = latestCatalogSnapshot.getSegments();

        Segment segmentToAdd = getSegment(mergeResult.getMergedWriterFileSet());
        Set<Segment> segmentsToRemove = new HashSet<>(oneMerge.getSegmentsToMerge());

        boolean inserted = false;
        int newSegIdx = 0;
        for (int segIdx = 0, cnt = segmentList.size(); segIdx < cnt; segIdx++) {
            assert segIdx >= newSegIdx;
            Segment currSegment = segmentList.get(segIdx);
            if(segmentsToRemove.contains(currSegment)) {
                if (!inserted) {
                    segmentList.set(segIdx, segmentToAdd);
                    inserted = true;
                    newSegIdx++;
                }
            } else {
                segmentList.set(newSegIdx, currSegment);
                newSegIdx++;
            }
        }

        // the rest of the segments in list are duplicates, so don't remove from map, only list!
        segmentList.subList(newSegIdx, segmentList.size()).clear();

        // Either we found place to insert segment, or, we did
        // not, but only because all segments we merged became
        // deleted while we are merging, in which case it should
        // be the case that the new segment is also all deleted,
        // we insert it at the beginning if it should not be dropped:
        if (!inserted) {
            segmentList.add(0, segmentToAdd);
        }
        CompositeEngineCatalogSnapshot newCatSnap = new CompositeEngineCatalogSnapshot(latestCatalogSnapshot.getId() + 1, latestCatalogSnapshot.getVersion() + 1, segmentList, catalogSnapshotMap, indexFileDeleter::get);

        // Note: userData will be populated in CompositeEngine.flush() before serialization
        // when this snapshot is committed to disk

        // Commit new catalog snapshot
        commitCatalogSnapshot(newCatSnap);
    }

    private synchronized void commitCatalogSnapshot(CompositeEngineCatalogSnapshot newCatSnap) {
        catalogSnapshotMap.put(newCatSnap.getId(), newCatSnap);
        if (latestCatalogSnapshot != null) {
            latestCatalogSnapshot.decRef();
        }
        latestCatalogSnapshot = newCatSnap;
        compositeEngineCommitter.addLuceneIndexes(latestCatalogSnapshot);
    }

    private Segment getSegment(Map<DataFormat, WriterFileSet> writerFileSetMap) {
        Segment segment = new Segment(0);

        for(DataFormat dataFormat : writerFileSetMap.keySet()) {
            segment.addSearchableFiles(dataFormat.name(), writerFileSetMap.get(dataFormat));
        }
        return segment;
    }

    private Optional<CompositeEngineCatalogSnapshot> getLastCommittedCatalogSnapshot() throws IOException {
        Map<String, String> lastCommittedData = compositeEngineCommitter.getLastCommittedData();
        if (lastCommittedData.containsKey(CATALOG_SNAPSHOT_KEY)) {
            return Optional.of(CompositeEngineCatalogSnapshot.deserializeFromString(lastCommittedData.get(CATALOG_SNAPSHOT_KEY)));
        }
        return Optional.empty();
    }

}
