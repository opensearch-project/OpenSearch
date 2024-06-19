/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.test.OpenSearchTestCase;

import static org.opensearch.index.remote.RemoteStoreEnums.DataCategory.TRANSLOG;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.DATA;
import static org.opensearch.index.remote.RemoteStoreEnums.DataType.LOCK_FILES;

public class RemoteStorePathStrategyTests extends OpenSearchTestCase {

    private static final BlobPath BASE_PATH = BlobPath.cleanPath().add("base-path");
    private static final String INDEX_UUID = "indexUUID";
    private static final String SHARD_ID = "shardId";

    public void testBasePathInput() {
        assertThrows(NullPointerException.class, () -> RemoteStorePathStrategy.BasePathInput.builder().build());
        assertThrows(NullPointerException.class, () -> RemoteStorePathStrategy.BasePathInput.builder().basePath(BASE_PATH).build());
        assertThrows(NullPointerException.class, () -> RemoteStorePathStrategy.BasePathInput.builder().indexUUID(INDEX_UUID).build());
        RemoteStorePathStrategy.BasePathInput input = RemoteStorePathStrategy.BasePathInput.builder()
            .basePath(BASE_PATH)
            .indexUUID(INDEX_UUID)
            .build();
        assertEquals(BASE_PATH, input.basePath());
        assertEquals(INDEX_UUID, input.indexUUID());
    }

    public void testPathInput() {
        assertThrows(NullPointerException.class, () -> RemoteStorePathStrategy.PathInput.builder().build());
        assertThrows(NullPointerException.class, () -> RemoteStorePathStrategy.PathInput.builder().shardId(SHARD_ID).build());
        assertThrows(
            NullPointerException.class,
            () -> RemoteStorePathStrategy.PathInput.builder().shardId(SHARD_ID).dataCategory(TRANSLOG).build()
        );

        // Translog Lock files - This is a negative case where the assertion will trip.
        assertThrows(
            AssertionError.class,
            () -> RemoteStorePathStrategy.PathInput.builder()
                .basePath(BASE_PATH)
                .indexUUID(INDEX_UUID)
                .shardId(SHARD_ID)
                .dataCategory(TRANSLOG)
                .dataType(LOCK_FILES)
                .build()
        );

        RemoteStorePathStrategy.PathInput input = RemoteStorePathStrategy.PathInput.builder()
            .basePath(BASE_PATH)
            .indexUUID(INDEX_UUID)
            .shardId(SHARD_ID)
            .dataCategory(TRANSLOG)
            .dataType(DATA)
            .build();
        assertEquals(BASE_PATH, input.basePath());
        assertEquals(INDEX_UUID, input.indexUUID());
        assertEquals(SHARD_ID, input.shardId());
        assertEquals(DATA, input.dataType());
        assertEquals(TRANSLOG, input.dataCategory());
    }

    public void testFixedSubPath() {
        RemoteStorePathStrategy.BasePathInput input = RemoteStorePathStrategy.BasePathInput.builder()
            .basePath(BASE_PATH)
            .indexUUID(INDEX_UUID)
            .build();
        assertEquals(BlobPath.cleanPath().add(INDEX_UUID), input.fixedSubPath());

        RemoteStorePathStrategy.PathInput input2 = RemoteStorePathStrategy.PathInput.builder()
            .basePath(BASE_PATH)
            .indexUUID(INDEX_UUID)
            .shardId(SHARD_ID)
            .dataCategory(TRANSLOG)
            .dataType(DATA)
            .build();
        assertEquals(BlobPath.cleanPath().add(INDEX_UUID).add(SHARD_ID).add(TRANSLOG.getName()).add(DATA.getName()), input2.fixedSubPath());

    }
}
