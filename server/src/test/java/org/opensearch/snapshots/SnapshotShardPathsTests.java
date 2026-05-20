/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.remote.RemoteStoreEnums;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class SnapshotShardPathsTests extends OpenSearchTestCase {

    public void testToXContent() throws IOException {
        List<String> paths = Arrays.asList("/path/to/shard/1", "/path/to/shard/2", "/path/to/shard/3");
        String indexId = "index-id";
        String indexName = "index-name";
        int numberOfShards = 5;
        RemoteStoreEnums.PathType shardPathType = RemoteStoreEnums.PathType.HASHED_PREFIX;
        RemoteStoreEnums.PathHashAlgorithm shardPathHashAlgorithm = RemoteStoreEnums.PathHashAlgorithm.FNV_1A_BASE64;

        SnapshotShardPaths snapshotShardPaths = new SnapshotShardPaths(
            paths,
            indexId,
            indexName,
            numberOfShards,
            shardPathType,
            shardPathHashAlgorithm
        );

        BytesReference bytes = XContentHelper.toXContent(snapshotShardPaths, XContentType.JSON, false);
        String expectedJson =
            "{\"indexId\":\"index-id\",\"indexName\":\"index-name\",\"number_of_shards\":5,\"shard_path_type\":1,\"shard_path_hash_algorithm\":0,\"paths\":[\"/path/to/shard/1\",\"/path/to/shard/2\",\"/path/to/shard/3\"]}";
        assertEquals(expectedJson, bytes.utf8ToString());
    }

    public void testMissingPaths() {
        List<String> paths = Collections.emptyList();
        String indexId = "index-id";
        String indexName = "index-name";
        int numberOfShards = 5;
        RemoteStoreEnums.PathType shardPathType = RemoteStoreEnums.PathType.FIXED;
        RemoteStoreEnums.PathHashAlgorithm shardPathHashAlgorithm = RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1;

        AssertionError exception = expectThrows(
            AssertionError.class,
            () -> new SnapshotShardPaths(paths, indexId, indexName, numberOfShards, shardPathType, shardPathHashAlgorithm)
        );
        assertTrue(exception.getMessage().contains("paths must not be empty"));
    }

    public void testMissingIndexId() {
        List<String> paths = Arrays.asList("/path/to/shard/1", "/path/to/shard/2", "/path/to/shard/3");
        String indexId = "";
        String indexName = "index-name";
        int numberOfShards = 5;
        RemoteStoreEnums.PathType shardPathType = RemoteStoreEnums.PathType.HASHED_PREFIX;
        RemoteStoreEnums.PathHashAlgorithm shardPathHashAlgorithm = RemoteStoreEnums.PathHashAlgorithm.FNV_1A_BASE64;

        AssertionError exception = expectThrows(
            AssertionError.class,
            () -> new SnapshotShardPaths(paths, indexId, indexName, numberOfShards, shardPathType, shardPathHashAlgorithm)
        );
        assertTrue(exception.getMessage().contains("indexId must not be empty"));
    }

    public void testMissingIndexName() {
        List<String> paths = Arrays.asList("/path/to/shard/1", "/path/to/shard/2", "/path/to/shard/3");
        String indexId = "index-id";
        String indexName = "";
        int numberOfShards = 5;
        RemoteStoreEnums.PathType shardPathType = RemoteStoreEnums.PathType.HASHED_PREFIX;
        RemoteStoreEnums.PathHashAlgorithm shardPathHashAlgorithm = RemoteStoreEnums.PathHashAlgorithm.FNV_1A_BASE64;

        AssertionError exception = expectThrows(
            AssertionError.class,
            () -> new SnapshotShardPaths(paths, indexId, indexName, numberOfShards, shardPathType, shardPathHashAlgorithm)
        );
        assertTrue(exception.getMessage().contains("indexName must not be empty"));
    }

    public void testMissingNumberOfShards() {
        List<String> paths = Arrays.asList("/path/to/shard/1", "/path/to/shard/2", "/path/to/shard/3");
        String indexId = "index-id";
        String indexName = "index-name";
        int numberOfShards = 0;
        RemoteStoreEnums.PathType shardPathType = RemoteStoreEnums.PathType.HASHED_PREFIX;
        RemoteStoreEnums.PathHashAlgorithm shardPathHashAlgorithm = RemoteStoreEnums.PathHashAlgorithm.FNV_1A_BASE64;

        AssertionError exception = expectThrows(
            AssertionError.class,
            () -> new SnapshotShardPaths(paths, indexId, indexName, numberOfShards, shardPathType, shardPathHashAlgorithm)
        );
        assertTrue(exception.getMessage().contains("numberOfShards must be > 0"));
    }

    public void testMissingShardPathType() {
        List<String> paths = Arrays.asList("/path/to/shard/1", "/path/to/shard/2", "/path/to/shard/3");
        String indexId = "index-id";
        String indexName = "index-name";
        int numberOfShards = 5;
        RemoteStoreEnums.PathType shardPathType = null;
        RemoteStoreEnums.PathHashAlgorithm shardPathHashAlgorithm = RemoteStoreEnums.PathHashAlgorithm.FNV_1A_BASE64;

        AssertionError exception = expectThrows(
            AssertionError.class,
            () -> new SnapshotShardPaths(paths, indexId, indexName, numberOfShards, shardPathType, shardPathHashAlgorithm)
        );
        assertTrue(exception.getMessage().contains("shardPathType must not be null"));
    }

    public void testMissingShardPathHashAlgorithm() {
        List<String> paths = Arrays.asList("/path/to/shard/1", "/path/to/shard/2", "/path/to/shard/3");
        String indexId = "index-id";
        String indexName = "index-name";
        int numberOfShards = 5;
        RemoteStoreEnums.PathType shardPathType = RemoteStoreEnums.PathType.HASHED_PREFIX;
        RemoteStoreEnums.PathHashAlgorithm shardPathHashAlgorithm = null;

        AssertionError exception = expectThrows(
            AssertionError.class,
            () -> new SnapshotShardPaths(paths, indexId, indexName, numberOfShards, shardPathType, shardPathHashAlgorithm)
        );
        assertEquals("shardPathHashAlgorithm must not be null", exception.getMessage());
    }

    public void testFromXContent() {
        UnsupportedOperationException exception = expectThrows(
            UnsupportedOperationException.class,
            () -> SnapshotShardPaths.fromXContent(null)
        );
        assertEquals("SnapshotShardPaths.fromXContent() is not supported", exception.getMessage());
    }
}
