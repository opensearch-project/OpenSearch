/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.encryption;

import java.util.Objects;

/**
 * Immutable composite key for PME data-key cache entries.
 *
 * <p>Follows the same pattern as {@code ShardCacheKey} in the Lucene storage-encryption
 * plugin. Equality and hashing are based on {@code indexUuid} and {@code shardId},
 * giving shard-level lifecycle control: each shard holds its own cache entry and can
 * be evicted independently when the shard closes. Multiple shards of the same index
 * on the same node each decrypt the index-level keyfile once and cache the result
 * separately, which keeps eviction simple — no reference counting of "how many shards
 * are still alive for this index" is required.
 *
 * <p>The {@code dataKeyId} field is carried for convenience (logging) but is
 * <em>not</em> part of key identity, mirroring how {@code ShardCacheKey} carries
 * {@code indexName} for logging without including it in {@code equals}/{@code hashCode}.
 *
 * <p>In v1 the only valid {@code dataKeyId} is {@link PmeFileKeyMetadata#DEFAULT_DATA_KEY_ID}.
 *
 * @opensearch.internal
 */
final class PmeCacheKey {

    private final String indexUuid;
    private final int shardId;
    /** Convenience field — NOT part of key identity. */
    private final String dataKeyId;
    private int hash; // lazy-computed, matches ShardCacheKey pattern

    PmeCacheKey(String indexUuid, int shardId, String dataKeyId) {
        this.indexUuid = Objects.requireNonNull(indexUuid, "indexUuid must not be null");
        this.shardId = shardId;
        this.dataKeyId = Objects.requireNonNull(dataKeyId, "dataKeyId must not be null");
    }

    String getIndexUuid() {
        return indexUuid;
    }

    int getShardId() {
        return shardId;
    }

    String getDataKeyId() {
        return dataKeyId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof PmeCacheKey)) return false;
        PmeCacheKey that = (PmeCacheKey) o;
        // dataKeyId is NOT part of equality — only uuid + shardId
        return shardId == that.shardId && indexUuid.equals(that.indexUuid);
    }

    @Override
    public int hashCode() {
        int h = hash;
        if (h == 0) {
            h = Objects.hash(indexUuid, shardId);
            if (h == 0) h = 1; // avoid zero sentinel, mirrors ShardCacheKey
            hash = h;
        }
        return h;
    }

    @Override
    public String toString() {
        return indexUuid + "/" + dataKeyId + "-shard-" + shardId;
    }
}
