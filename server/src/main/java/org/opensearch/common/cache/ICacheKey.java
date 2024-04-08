/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.List;

/**
 * A key wrapper used for ICache implementations, which carries dimensions with it.
 * @param <K> the type of the underlying key
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class ICacheKey<K> {
    public final K key; // K must implement equals()
    public final List<String> dimensions; // Dimension values. The dimension names are implied.
    /**
     * If this key is invalidated and dropDimensions is true, the ICache implementation will also drop all stats,
     * including hits/misses/evictions, with this combination of dimension values.
     */
    private boolean dropStatsForDimensions;

    /**
     * Constructor to use when specifying dimensions.
     */
    public ICacheKey(K key, List<String> dimensions) {
        this.key = key;
        this.dimensions = dimensions;
    }

    /**
     * Constructor to use when no dimensions are needed.
     */
    public ICacheKey(K key) {
        this.key = key;
        this.dimensions = List.of();
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o == null) {
            return false;
        }
        if (o.getClass() != ICacheKey.class) {
            return false;
        }
        ICacheKey other = (ICacheKey) o;
        return key.equals(other.key) && dimensions.equals(other.dimensions);
    }

    @Override
    public int hashCode() {
        return 31 * key.hashCode() + dimensions.hashCode();
    }

    // As K might not be Accountable, directly pass in its memory usage to be added.
    public long ramBytesUsed(long underlyingKeyRamBytes) {
        long estimate = underlyingKeyRamBytes;
        for (String dim : dimensions) {
            estimate += dim.length();
        }
        return estimate;
    }

    public boolean getDropStatsForDimensions() {
        return dropStatsForDimensions;
    }

    public void setDropStatsForDimensions(boolean newValue) {
        dropStatsForDimensions = newValue;
    }
}
