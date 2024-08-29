/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.remote.RemoteStoreEnums.PathHashAlgorithm;
import org.opensearch.index.remote.RemoteStoreEnums.PathType;
import org.opensearch.repositories.IndexId;

import java.io.IOException;
import java.util.List;

/**
 * Snapshot Shard path information.
 *
 * @opensearch.internal
 */
public class SnapshotShardPaths implements ToXContent {

    public static final String DIR = "snapshot_shard_paths";

    public static final String DELIMITER = "#";

    public static final String FILE_NAME_FORMAT = "%s";

    private static final String PATHS_FIELD = "paths";
    private static final String INDEX_ID_FIELD = "indexId";
    private static final String INDEX_NAME_FIELD = "indexName";
    private static final String NUMBER_OF_SHARDS_FIELD = "number_of_shards";
    private static final String SHARD_PATH_TYPE_FIELD = "shard_path_type";
    private static final String SHARD_PATH_HASH_ALGORITHM_FIELD = "shard_path_hash_algorithm";

    private final List<String> paths;
    private final String indexId;
    private final String indexName;
    private final int numberOfShards;
    private final PathType shardPathType;
    private final PathHashAlgorithm shardPathHashAlgorithm;

    public SnapshotShardPaths(
        List<String> paths,
        String indexId,
        String indexName,
        int numberOfShards,
        PathType shardPathType,
        PathHashAlgorithm shardPathHashAlgorithm
    ) {
        assert !paths.isEmpty() : "paths must not be empty";
        assert indexId != null && !indexId.isEmpty() : "indexId must not be empty";
        assert indexName != null && !indexName.isEmpty() : "indexName must not be empty";
        assert numberOfShards > 0 : "numberOfShards must be > 0";
        assert shardPathType != null : "shardPathType must not be null";
        assert shardPathHashAlgorithm != null : "shardPathHashAlgorithm must not be null";

        this.paths = paths;
        this.indexId = indexId;
        this.indexName = indexName;
        this.numberOfShards = numberOfShards;
        this.shardPathType = shardPathType;
        this.shardPathHashAlgorithm = shardPathHashAlgorithm;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(INDEX_ID_FIELD, indexId);
        builder.field(INDEX_NAME_FIELD, indexName);
        builder.field(NUMBER_OF_SHARDS_FIELD, numberOfShards);
        builder.field(SHARD_PATH_TYPE_FIELD, shardPathType.getCode());
        builder.field(SHARD_PATH_HASH_ALGORITHM_FIELD, shardPathHashAlgorithm.getCode());
        builder.startArray(PATHS_FIELD);
        for (String path : paths) {
            builder.value(path);
        }
        builder.endArray();
        return builder;
    }

    public static SnapshotShardPaths fromXContent(XContentParser ignored) {
        throw new UnsupportedOperationException("SnapshotShardPaths.fromXContent() is not supported");
    }

    /**
     * Parses a shard path string and extracts relevant shard information.
     *
     * @param shardPath The shard path string to parse. Expected format is:
     *                  [index_id]#[index_name]#[shard_count]#[path_type_code]#[path_hash_algorithm_code]
     * @return A {@link ShardInfo} object containing the parsed index ID and shard count.
     * @throws IllegalArgumentException if the shard path format is invalid or cannot be parsed.
     */
    public static ShardInfo parseShardPath(String shardPath) {
        String[] parts = shardPath.split(SnapshotShardPaths.DELIMITER);
        if (parts.length != 5) {
            throw new IllegalArgumentException("Invalid shard path format: " + shardPath);
        }
        try {
            IndexId indexId = new IndexId(parts[1], parts[0], Integer.parseInt(parts[3]));
            int shardCount = Integer.parseInt(parts[2]);
            return new ShardInfo(indexId, shardCount);
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid shard path format: " + shardPath, e);
        }
    }

    /**
     * Represents parsed information from a shard path.
     * This class encapsulates the index ID and shard count extracted from a shard path string.
     */
    public static class ShardInfo {
        /** The ID of the index associated with this shard. */
        private final IndexId indexId;

        /** The total number of shards for this index. */
        private final int shardCount;

        /**
         * Constructs a new ShardInfo instance.
         *
         * @param indexId    The ID of the index associated with this shard.
         * @param shardCount The total number of shards for this index.
         */
        public ShardInfo(IndexId indexId, int shardCount) {
            this.indexId = indexId;
            this.shardCount = shardCount;
        }

        public IndexId getIndexId() {
            return indexId;
        }

        public int getShardCount() {
            return shardCount;
        }
    }
}
