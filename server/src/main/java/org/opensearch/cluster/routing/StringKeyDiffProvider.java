/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.routing;

import org.opensearch.cluster.DiffableUtils;

import java.util.Map;

/**
 * Interface for providing a difference (diff) between two maps with {@code String} keys and values of type {@code V}.
 * This interface is used to compute and obtain the difference between two versions of a map, typically used
 * in cluster state updates or other scenarios where changes need to be tracked and propagated efficiently.
 *
 * @param <V> the type of the values in the map
 */
public interface StringKeyDiffProvider<V> {

    /**
     * Provides the difference between two versions of a map with {@code String} keys and values of type {@code V}.
     * The difference is represented as a {@link DiffableUtils.MapDiff} object, which can be used to apply the
     * changes to another map or to serialize the diff.
     *
     * @return a {@link DiffableUtils.MapDiff} object representing the difference between the maps
     */
    DiffableUtils.MapDiff<String, V, Map<String, V>> provideDiff();

}
