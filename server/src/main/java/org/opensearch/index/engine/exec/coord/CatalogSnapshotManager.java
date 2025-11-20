/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.coord;

import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.FileMetadata;
import org.opensearch.index.engine.exec.RefreshResult;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.commit.Committer;
import org.opensearch.index.engine.exec.merge.MergeResult;
import org.opensearch.index.engine.exec.merge.OneMerge;
import org.opensearch.index.shard.ShardPath;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

import static org.opensearch.index.engine.exec.coord.CatalogSnapshot.CATALOG_SNAPSHOT_KEY;
import static org.opensearch.index.engine.exec.coord.CatalogSnapshot.LAST_COMPOSITE_WRITER_GEN_KEY;

public class CatalogSnapshotManager {

    private CatalogSnapshot latestCatalogSnapshot;
    private final Committer compositeEngineCommitter;
    private final Map<Long, CatalogSnapshot> catalogSnapshotMap;
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
            latestCatalogSnapshot = new CatalogSnapshot(1, new ArrayList<>(), catalogSnapshotMap, indexFileDeleter::get);
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
        CatalogSnapshot newCatSnap;
        newCatSnap = new CatalogSnapshot(latestCatalogSnapshot.getId()+1, refreshResult.getRefreshedSegments(), catalogSnapshotMap, indexFileDeleter::get);
        commitCatalogSnapshot(newCatSnap);
    }

    public synchronized void applyMergeResults(MergeResult mergeResult, OneMerge oneMerge) {

        List<CatalogSnapshot.Segment> segmentList = latestCatalogSnapshot.getSegments();

        CatalogSnapshot.Segment segmentToAdd = getSegment(mergeResult.getMergedWriterFileSet());

        Set<FileMetadata> filesToRemove = new HashSet<>();
        oneMerge.getFilesToMerge().forEach(file -> filesToRemove.add(file));

        boolean inserted = false;
        int newSegIdx = 0;
        for (int segIdx = 0, cnt = segmentList.size(); segIdx < cnt; segIdx++) {
            assert segIdx >= newSegIdx;
            CatalogSnapshot.Segment currSegment = segmentList.get(segIdx);
            if(filesToRemove.containsAll(currSegment.getSearchableFiles(oneMerge.getDataFormat().name()))) {
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
        // not, but only because all segments we merged becamee
        // deleted while we are merging, in which case it should
        // be the case that the new segment is also all deleted,
        // we insert it at the beginning if it should not be dropped:
        if (!inserted) {
            segmentList.add(0, segmentToAdd);
        }
        CatalogSnapshot newCatSnap = new CatalogSnapshot(latestCatalogSnapshot.getId()+1, segmentList, catalogSnapshotMap, indexFileDeleter::get);

        // Commit new catalog snapshot
        commitCatalogSnapshot(newCatSnap);
    }

    private synchronized void commitCatalogSnapshot(CatalogSnapshot newCatSnap) {
        catalogSnapshotMap.put(newCatSnap.getId(), newCatSnap);
        if (latestCatalogSnapshot != null) {
            latestCatalogSnapshot.decRef();
        }
        latestCatalogSnapshot = newCatSnap;
        compositeEngineCommitter.addLuceneIndexes(latestCatalogSnapshot);
    }

    private CatalogSnapshot.Segment getSegment(Map<DataFormat, WriterFileSet> writerFileSetMap) {
        CatalogSnapshot.Segment segment = new CatalogSnapshot.Segment(0);

        for(DataFormat dataFormat : writerFileSetMap.keySet()) {
            segment.addSearchableFiles(dataFormat.name(), writerFileSetMap.get(dataFormat));
        }
        return segment;
    }

    private Optional<CatalogSnapshot> getLastCommittedCatalogSnapshot() throws IOException {
        Map<String, String> lastCommittedData = compositeEngineCommitter.getLastCommittedData();
        if (lastCommittedData.containsKey(CATALOG_SNAPSHOT_KEY)) {
            return Optional.of(CatalogSnapshot.deserializeFromString(lastCommittedData.get(CATALOG_SNAPSHOT_KEY)));
        }
        return Optional.empty();
    }

    private long getLastCommittedCatalogSnapshotId() throws IOException {
        long lastCommittedSnapshotId = -1;
        Map<String,String> lastCommittedData =  this.compositeEngineCommitter.getLastCommittedData();
        if (lastCommittedData.containsKey(LAST_COMPOSITE_WRITER_GEN_KEY)) {
            lastCommittedSnapshotId = Long.parseLong(lastCommittedData.get(LAST_COMPOSITE_WRITER_GEN_KEY));
        }
        return lastCommittedSnapshotId;
    }

}
