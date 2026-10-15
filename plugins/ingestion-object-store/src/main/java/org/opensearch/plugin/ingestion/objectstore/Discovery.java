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
 * How a shard discovers the objects it owns.
 * <ul>
 *     <li>{@link #MANIFEST} reads the explicit {@code manifest} parameter frozen in the index metadata at creation; no
 *     LIST is issued, but the whole manifest is carried in the cluster state and materialized in every shard.</li>
 *     <li>{@link #LIST} has each shard list only its own sub-prefix at shard start, so the cluster state carries no
 *     object keys and a shard holds only what it owns. Requires list permission on the store.</li>
 * </ul>
 */
enum Discovery {
    /**
     * Read the object set from the explicit {@code manifest} parameter; no LIST is issued.
     */
    MANIFEST("manifest"),

    /**
     * Have each shard list its own {@code <prefix>/<shardId>/} sub-prefix at shard start.
     */
    LIST("list");

    private final String name;

    Discovery(String name) {
        this.name = name;
    }

    /**
     * Returns the lowercase configuration name for this discovery mode.
     *
     * @return the configuration name
     */
    public String getName() {
        return name;
    }

    /**
     * Parses a discovery mode from its configuration name.
     *
     * @param value the configuration name (case-insensitive), or {@code null} for the default {@link #MANIFEST}
     * @return the matching discovery mode
     * @throws IllegalArgumentException if the value does not match a known discovery mode
     */
    public static Discovery fromString(String value) {
        if (value == null) {
            return ObjectStoreSourceConfig.DEFAULT_DISCOVERY;
        }
        for (Discovery discovery : values()) {
            if (discovery.name.equalsIgnoreCase(value)) {
                return discovery;
            }
        }
        throw new IllegalArgumentException(
            String.format(Locale.ROOT, "Unknown discovery [%s]. Valid values are %s", value, Arrays.toString(values()))
        );
    }

    @Override
    public String toString() {
        return name;
    }
}
