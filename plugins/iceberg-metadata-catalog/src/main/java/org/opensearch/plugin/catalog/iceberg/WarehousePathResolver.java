/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.catalog.iceberg;

import java.util.Objects;

/**
 * Derives the deterministic warehouse key for a remote-store file.
 * <p>
 * The remote-store filename is already content-addressed (Lucene logical name suffixed
 * with a per-upload UUID, e.g. {@code _0.cfe__gX7bNIIBrs0AUNsR2yEG}), so passing it
 * through unchanged gives us an S3 key that is stable across retries: same source
 * bytes → same key → S3 PUT is idempotent. This is the layer-3 correctness primitive
 * called out in the design doc.
 * <p>
 * Pure helper with a single static method. Kept in its own class so the behavior can
 * be unit-tested without a live catalog.
 */
final class WarehousePathResolver {

    /** Prefix under which all per-index, per-shard parquet data is stored. */
    static final String DATA_PREFIX = "data";

    private WarehousePathResolver() {}

    /**
     * Builds the warehouse-relative path for a single remote-store file.
     *
     * @param indexUUID the source OpenSearch index UUID (read from the Iceberg table's
     *                  {@code opensearch.index_uuid} property by {@link PublishContext})
     * @param shardId the primary shard id owning the file
     * @param remoteFilename the remote-store filename as returned by
     *                       {@code RemoteSegmentMetadata.getMetadata().keySet()}.
     *                       Passed through unchanged — the remote-store layer already
     *                       guarantees per-upload uniqueness via its UUID suffix.
     * @return a warehouse-relative path of the form
     *         {@code data/<indexUUID>/<shardId>/<remoteFilename>}. Callers combine
     *         this with {@code Table.location()} to build the absolute S3 URI handed
     *         to {@code S3FileIO.newOutputFile}.
     */
    static String resolve(String indexUUID, int shardId, String remoteFilename) {
        Objects.requireNonNull(indexUUID, "indexUUID");
        Objects.requireNonNull(remoteFilename, "remoteFilename");
        if (indexUUID.isEmpty()) {
            throw new IllegalArgumentException("indexUUID must not be empty");
        }
        if (remoteFilename.isEmpty()) {
            throw new IllegalArgumentException("remoteFilename must not be empty");
        }
        if (shardId < 0) {
            throw new IllegalArgumentException("shardId must be non-negative, got " + shardId);
        }
        return DATA_PREFIX + "/" + indexUUID + "/" + shardId + "/" + remoteFilename;
    }

    /**
     * Returns the partition prefix (without the filename) for the given
     * {@code (indexUUID, shardId)} partition. Useful when constructing
     * partition-scoped scans or absolute URIs that target the partition directory.
     */
    static String partitionPrefix(String indexUUID, int shardId) {
        Objects.requireNonNull(indexUUID, "indexUUID");
        if (indexUUID.isEmpty()) {
            throw new IllegalArgumentException("indexUUID must not be empty");
        }
        if (shardId < 0) {
            throw new IllegalArgumentException("shardId must be non-negative, got " + shardId);
        }
        return DATA_PREFIX + "/" + indexUUID + "/" + shardId + "/";
    }
}
