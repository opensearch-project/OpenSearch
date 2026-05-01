/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.blockcache.spi;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Objects;

/**
 * Identifier for entries in a {@link org.opensearch.blockcache.BlockCache}.
 *
 * <p>Namespaces cache entries by the tuple
 * {@code (repositoryType, repositoryName, objectKey, rangeStart, rangeEnd)} so that
 * multiple repositories sharing a single node-level cache cannot collide on the
 * same keyspace.
 *
 * <p>All four identity dimensions are required:
 * <ul>
 *   <li>{@code repositoryType} (e.g. {@code "s3"}, {@code "gcs"}, {@code "azure"},
 *       {@code "fs"}) distinguishes entries that belong to different backend
 *       implementations. Two repositories of different types never share keys
 *       even if every other component is identical.</li>
 *   <li>{@code repositoryName} (from {@code RepositoryMetadata#name}) distinguishes
 *       multiple repositories of the same type registered on the same node.</li>
 *   <li>{@code objectKey} is the bucket-relative object path within a repository.</li>
 *   <li>{@code rangeStart} (inclusive) and {@code rangeEnd} (exclusive) identify a
 *       specific byte range within the object. Block granularity is set by the
 *       calling layer (Parquet column chunks, Lucene file ranges, etc.).</li>
 * </ul>
 *
 * <p>Immutable. {@link #equals} and {@link #hashCode} are derived by the record
 * from all five components.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public record BlockCacheKey(String repositoryType, String repositoryName, String objectKey, long rangeStart, long rangeEnd) {
    /**
     * Compact constructor enforcing non-null string components and a strictly
     * positive range width.
     *
     * @throws NullPointerException     if {@code repositoryType}, {@code repositoryName},
     *                                  or {@code objectKey} is null
     * @throws IllegalArgumentException if {@code rangeEnd <= rangeStart}
     */
    public BlockCacheKey {
        Objects.requireNonNull(repositoryType);
        Objects.requireNonNull(repositoryName);
        Objects.requireNonNull(objectKey);
        if (rangeEnd <= rangeStart) {
            throw new IllegalArgumentException("rangeEnd must be > rangeStart");
        }
    }
}
