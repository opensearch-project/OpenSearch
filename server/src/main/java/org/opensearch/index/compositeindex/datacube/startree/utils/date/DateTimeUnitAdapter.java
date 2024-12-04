/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.utils.date;

import org.opensearch.common.Rounding;

import java.util.Objects;

/**
 * Adapter class to convert {@link Rounding.DateTimeUnit} to {@link DateTimeUnitRounding}
 *
 * @opensearch.experimental
 */
public class DateTimeUnitAdapter implements DateTimeUnitRounding {
    private final Rounding.DateTimeUnit dateTimeUnit;

    public DateTimeUnitAdapter(Rounding.DateTimeUnit dateTimeUnit) {
        this.dateTimeUnit = dateTimeUnit;
    }

    @Override
    public String shortName() {
        return dateTimeUnit.shortName();
    }

    @Override
    public long roundFloor(long utcMillis) {
        return dateTimeUnit.roundFloor(utcMillis);
    }

    /**
     * Checks if this DateTimeUnitRounding is equal to another object.
     * Two DateTimeUnitRounding instances are considered equal if they have the same short name.
     *
     * @param obj The object to compare with
     * @return true if the objects are equal, false otherwise
     */
    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof DateTimeUnitRounding)) return false;
        DateTimeUnitRounding other = (DateTimeUnitRounding) obj;
        return Objects.equals(shortName(), other.shortName());
    }

    /**
     * Returns a hash code value for the object.
     * This method is implemented to be consistent with equals.
     *
     * @return a hash code value for this object
     */
    @Override
    public int hashCode() {
        return Objects.hash(shortName());
    }
}
