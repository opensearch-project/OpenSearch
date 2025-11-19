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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

public class CatalogSnapshotManager {

    private CatalogSnapshot catalogSnapshot;
    private final Committer compositeEngineCommitter;
    private final Map<Long, CatalogSnapshot> catalogSnapshotMap;
    private final AtomicReference<IndexFileDeleter> indexFileDeleter;

    public CatalogSnapshotManager(Committer compositeEngineCommitter, CatalogSnapshot lastCommitedSnapshot, AtomicReference<IndexFileDeleter> indexFileDeleter) {
        catalogSnapshotMap = new HashMap<>();
        this.compositeEngineCommitter = compositeEngineCommitter;
        this.indexFileDeleter = indexFileDeleter;
        if(lastCommitedSnapshot!=null) {
            this.catalogSnapshot = lastCommitedSnapshot;
            this.catalogSnapshot.setCatalogSnapshotMap(catalogSnapshotMap);
        } else {
            this.catalogSnapshot = new CatalogSnapshot(1, new ArrayList<>(), catalogSnapshotMap, indexFileDeleter::get);
        }
        catalogSnapshotMap.put(catalogSnapshot.getId(), catalogSnapshot);

    }

    public CatalogSnapshot getCatalogSnapshotFromId(long catalogSnapshotId) {
        return catalogSnapshotMap.get(catalogSnapshotId);
    }

    public CompositeEngine.ReleasableRef<CatalogSnapshot> acquireSnapshot() {
        final CatalogSnapshot snapshot = catalogSnapshot;
        snapshot.incRef(); // this should be package-private
        return new CompositeEngine.ReleasableRef<CatalogSnapshot>(snapshot) {
            @Override
            public void close() throws Exception {
                snapshot.decRef(); // this should be package-private
            }
        };
    }

    public synchronized CatalogSnapshot applyRefreshResult(RefreshResult refreshResult) {
        CatalogSnapshot newCatSnap;
        newCatSnap = new CatalogSnapshot(catalogSnapshot.getId()+1, refreshResult.getRefreshedSegments(), catalogSnapshotMap, indexFileDeleter::get);
        commitCatalogSnapshot(newCatSnap);

        return newCatSnap;
    }

    public synchronized CatalogSnapshot applyMergeResults(MergeResult mergeResult, OneMerge oneMerge) {

        List<CatalogSnapshot.Segment> segmentList = catalogSnapshot.getSegments();

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
        CatalogSnapshot newCatSnap = new CatalogSnapshot(catalogSnapshot.getId()+1, segmentList, catalogSnapshotMap, indexFileDeleter::get);

        // Commit new catalog snapshot
        commitCatalogSnapshot(newCatSnap);

        return newCatSnap;
    }

    private synchronized void commitCatalogSnapshot(CatalogSnapshot newCatSnap) {
        catalogSnapshotMap.put(newCatSnap.getId(), newCatSnap);
        if (catalogSnapshot != null) {
            catalogSnapshot.decRef();
        }
        catalogSnapshot = newCatSnap;
        compositeEngineCommitter.addLuceneIndexes(catalogSnapshot);
    }

    private CatalogSnapshot.Segment getSegment(Map<DataFormat, WriterFileSet> writerFileSetMap) {
        CatalogSnapshot.Segment segment = new CatalogSnapshot.Segment(0);

        for(DataFormat dataFormat : writerFileSetMap.keySet()) {
            segment.addSearchableFiles(dataFormat.name(), writerFileSetMap.get(dataFormat));
        }
        return segment;
    }
}
