/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.snapshots.list;

import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.snapshots.IndexShardSnapshotStatus;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.Repository;
import org.opensearch.repositories.RepositoryData;
import org.opensearch.snapshots.SnapshotId;
import org.opensearch.snapshots.SnapshotInfo;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Utilities for snapshot listing that read only page-relevant data from repository.
 *
 * @opensearch.internal
 */
final class SnapshotListUtils {

    static class IndexFileStats {
        static final IndexFileStats EMPTY = new IndexFileStats(0L, 0L);
        final long fileCount;
        final long sizeInBytes;

        IndexFileStats(long fileCount, long sizeInBytes) {
            this.fileCount = fileCount;
            this.sizeInBytes = sizeInBytes;
        }
    }

    private SnapshotListUtils() {}

    static Map<String, IndexMetadata> loadIndexMetadata(
        Repository repo,
        RepositoryData repositoryData,
        SnapshotInfo snapshotInfo,
        List<String> indices
    ) throws IOException {
        Map<String, IndexMetadata> result = new HashMap<>();
        SnapshotId snapshotId = snapshotInfo.snapshotId();
        for (String index : indices) {
            IndexId indexId = repositoryData.resolveIndexId(index);
            IndexMetadata meta = repo.getSnapshotIndexMetaData(repositoryData, snapshotId, indexId);
            if (meta != null) {
                result.put(index, meta);
            }
        }
        return result;
    }

    static Map<String, IndexFileStats> loadIndexFileStats(
        Repository repo,
        RepositoryData repositoryData,
        SnapshotInfo snapshotInfo,
        List<String> indices,
        Map<String, IndexMetadata> indexMetadataMap
    ) throws IOException {
        Map<String, IndexFileStats> result = new HashMap<>();
        SnapshotId snapshotId = snapshotInfo.snapshotId();
        for (String index : indices) {
            IndexMetadata meta = indexMetadataMap.get(index);
            int numShards = meta == null ? 0 : meta.getNumberOfShards();
            long files = 0L;
            long bytes = 0L;
            if (numShards > 0) {
                IndexId indexId = repositoryData.resolveIndexId(index);
                for (int shard = 0; shard < numShards; shard++) {
                    IndexShardSnapshotStatus.Copy s = repo.getShardSnapshotStatus(snapshotId, indexId, new ShardId(meta.getIndex(), shard))
                        .asCopy();
                    files += s.getTotalFileCount();
                    bytes += s.getTotalSize();
                }
            }
            result.put(index, new IndexFileStats(files, bytes));
        }
        return result;
    }
}


