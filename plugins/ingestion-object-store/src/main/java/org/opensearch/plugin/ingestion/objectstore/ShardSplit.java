/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import java.util.Arrays;
import java.util.Locale;

/**
 * The strategy used to divide the source object(s) into disjoint slices, one per source partition.
 * <ul>
 *     <li>{@link #EQUAL_BYTES} divides a single object into contiguous byte ranges, one per shard.</li>
 *     <li>{@link #WHOLE_FILES} assigns each shard a disjoint set of whole objects under a prefix.</li>
 *     <li>{@link #SHARD_PREFIXES} assigns each shard the sub-prefix {@code <prefix>/<shardId>/}.</li>
 * </ul>
 */
enum ShardSplit {
    /**
     * Divide a single object into contiguous byte ranges resolved to line boundaries, one per shard.
     */
    EQUAL_BYTES("equal_bytes"),

    /**
     * Assign each shard a disjoint set of whole objects under a prefix, mapped by hash.
     */
    WHOLE_FILES("whole_files"),

    /**
     * Assign each shard the dedicated sub-prefix {@code <prefix>/<shardId>/}.
     */
    SHARD_PREFIXES("shard_prefixes");

    private final String name;

    ShardSplit(String name) {
        this.name = name;
    }

    /**
     * Returns the lowercase configuration name for this split strategy.
     *
     * @return the configuration name
     */
    public String getName() {
        return name;
    }

    /**
     * Parses a shard split from its configuration name.
     *
     * @param value the configuration name (case-insensitive)
     * @return the matching shard split
     * @throws IllegalArgumentException if the value does not match a known split strategy
     */
    public static ShardSplit fromString(String value) {
        if (value == null) {
            throw new IllegalArgumentException("shard_split is required and must be one of " + Arrays.toString(values()));
        }
        for (ShardSplit split : values()) {
            if (split.name.equalsIgnoreCase(value)) {
                return split;
            }
        }
        throw new IllegalArgumentException(
            String.format(Locale.ROOT, "Unknown shard_split [%s]. Valid values are %s", value, Arrays.toString(values()))
        );
    }

    @Override
    public String toString() {
        return name;
    }
}
