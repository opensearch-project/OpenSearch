/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.stats;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;
import java.util.Objects;

public class CacheStatsDimension implements Writeable {
    // Values for tier dimensions, that are reused across CacheStats implementations
    public static final String TIER_DIMENSION_NAME = "tier";
    public final String dimensionName;
    public final String dimensionValue;

    public CacheStatsDimension(String dimensionName, String dimensionValue) {
        this.dimensionName = dimensionName;
        this.dimensionValue = dimensionValue;
    }

    public CacheStatsDimension(StreamInput in) throws IOException {
        this.dimensionName = in.readString();
        this.dimensionValue = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(dimensionName);
        out.writeString(dimensionValue);
    }

    @Override
    public boolean equals(Object o) {
        if (o == this) {
            return true;
        }
        if (o == null) {
            return false;
        }
        if (o.getClass() != CacheStatsDimension.class) {
            return false;
        }
        CacheStatsDimension other = (CacheStatsDimension) o;
        if (other.dimensionName == null || other.dimensionValue == null) {
            return false;
        }
        return other.dimensionName.equals(dimensionName) && other.dimensionValue.equals(dimensionValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(dimensionName, dimensionValue);
    }
}
