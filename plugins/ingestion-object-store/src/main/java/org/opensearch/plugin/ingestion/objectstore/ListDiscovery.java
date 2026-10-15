/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobMetadata;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Discovers the objects under a single shard's sub-prefix for {@link Discovery#LIST}, so a shard never reads another
 * shard's keys.
 *
 * <p>The walk descends one path segment at a time through {@link BlobContainer#children()} rather than passing a
 * slash-bearing prefix to {@link BlobContainer#listBlobsByPrefix(String)}, because a directory-backed store treats path
 * separators as real directories while a flat key-value store does not. Nested directories are included depth-first up
 * to {@link #MAX_DEPTH}, and the result is sorted so slice order is deterministic.
 *
 * <p>Every list operation is charged against {@code max_list_requests} at {@link #KEYS_PER_LIST_PAGE} entries per billed
 * request, since the blob abstraction paginates internally.
 */
final class ListDiscovery {

    /**
     * Maximum directory depth walked below a shard's sub-prefix.
     */
    static final int MAX_DEPTH = 64;

    /**
     * Entries per list response page, used to derive the billed request count from a result size.
     */
    static final int KEYS_PER_LIST_PAGE = 1000;

    private ListDiscovery() {}

    /**
     * Lists the objects under a shard's sub-prefix and returns them as whole-object slices.
     *
     * @param container       the root container the consumer reads through
     * @param subPrefix       the shard's sub-prefix relative to {@code container}, ending in {@code /} (or empty)
     * @param maxObjects      the hard cap on the number of objects a shard may own
     * @param maxListRequests the cost cap on billed list requests
     * @return the discovered slices in object-name order, empty if the sub-prefix holds no objects
     * @throws IOException                if the store cannot be listed
     * @throws CostLimitExceededException if {@code maxListRequests} is exceeded
     * @throws IllegalArgumentException if more than {@code maxObjects} objects are found, the layout is deeper than
     *                                  {@link #MAX_DEPTH}, or {@code subPrefix} is not a canonical prefix
     */
    static List<ObjectSlice> discover(
        final BlobContainer container,
        final String subPrefix,
        final long maxObjects,
        final long maxListRequests
    ) throws IOException {
        Objects.requireNonNull(container, "container must not be null for discovery=list");
        Objects.requireNonNull(subPrefix, "subPrefix must not be null");
        if (subPrefix.isEmpty() == false && subPrefix.endsWith("/") == false) {
            throw new IllegalArgumentException("subPrefix must be empty or end with [/], got [" + LogSanitizer.sanitize(subPrefix) + "]");
        }
        if (subPrefix.startsWith("/") || subPrefix.contains("//")) {
            throw new IllegalArgumentException(
                "subPrefix must not have a leading [/] or an empty path segment [//], got [" + LogSanitizer.sanitize(subPrefix) + "]"
            );
        }
        final Walk walk = new Walk(subPrefix, maxObjects, maxListRequests);
        final BlobContainer shardContainer = descend(container, subPrefix, walk.budget);
        if (shardContainer != null) {
            collect(shardContainer, subPrefix, walk, 0);
            walk.slices.sort(Comparator.comparing(ObjectSlice::getObjectName, ObjectStoreManifest.BYTE_ORDER));
        }
        return walk.slices;
    }

    private static BlobContainer descend(final BlobContainer container, final String subPrefix, final ListBudget budget)
        throws IOException {
        BlobContainer current = container;
        for (final String segment : subPrefix.split("/")) {
            if (segment.isEmpty()) {
                continue;
            }
            final Map<String, BlobContainer> children = current.children();
            budget.chargeFor(children.size());
            current = children.get(segment);
            if (current == null) {
                return null;
            }
        }
        return current;
    }

    private static void collect(final BlobContainer container, final String relativePrefix, final Walk walk, final int depth)
        throws IOException {
        if (depth > MAX_DEPTH) {
            throw new IllegalArgumentException(
                "["
                    + ObjectStoreSourceConfig.DISCOVERY
                    + "="
                    + Discovery.LIST.getName()
                    + "] exceeded the maximum directory depth ["
                    + MAX_DEPTH
                    + "] at ["
                    + LogSanitizer.sanitize(relativePrefix)
                    + "]"
            );
        }
        final Map<String, BlobMetadata> blobs = container.listBlobs();
        walk.budget.chargeFor(blobs.size());
        if ((long) walk.slices.size() + blobs.size() > walk.maxObjects) {
            throw new IllegalArgumentException(
                "["
                    + ObjectStoreSourceConfig.DISCOVERY
                    + "="
                    + Discovery.LIST.getName()
                    + "] found more than ["
                    + ObjectStoreSourceConfig.MAX_OBJECTS
                    + "="
                    + walk.maxObjects
                    + "] objects in total under ["
                    + LogSanitizer.sanitize(walk.rootPrefix)
                    + "]; the cap is cumulative across the whole sub-prefix and was reached while listing ["
                    + LogSanitizer.sanitize(relativePrefix)
                    + "]"
            );
        }
        for (final BlobMetadata blob : blobs.values()) {
            walk.slices.add(new ObjectSlice(relativePrefix + blob.name(), 0, blob.length(), blob.length()));
        }
        final Map<String, BlobContainer> children = container.children();
        walk.budget.chargeFor(children.size());
        for (final Map.Entry<String, BlobContainer> child : children.entrySet()) {
            collect(child.getValue(), relativePrefix + child.getKey() + "/", walk, depth + 1);
        }
    }

    /**
     * The mutable state threaded through the recursive walk: what has been found, the sub-prefix the walk was rooted at,
     * the cardinality cap it is checked against, and the list budget it spends.
     */
    private static final class Walk {
        private final List<ObjectSlice> slices = new ArrayList<>();
        private final String rootPrefix;
        private final long maxObjects;
        private final ListBudget budget;

        Walk(final String rootPrefix, final long maxObjects, final long maxListRequests) {
            this.rootPrefix = rootPrefix;
            this.maxObjects = maxObjects;
            this.budget = new ListBudget(maxListRequests);
        }
    }

    /**
     * Counts billed list requests and fails the walk once the configured cap is exceeded.
     */
    private static final class ListBudget {
        private final long maxListRequests;
        private long used;

        ListBudget(final long maxListRequests) {
            this.maxListRequests = maxListRequests;
        }

        void chargeFor(final int entriesReturned) throws CostLimitExceededException {
            used += Math.max(1L, Math.ceilDiv((long) entriesReturned, KEYS_PER_LIST_PAGE));
            if (used > maxListRequests) {
                throw new CostLimitExceededException(
                    "list request count [" + used + "] exceeds max_list_requests [" + maxListRequests + "]"
                );
            }
        }
    }
}
