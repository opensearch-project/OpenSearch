/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube;

import org.opensearch.index.compositeindex.datacube.startree.utils.date.DateTimeUnitRounding;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.unmodifiableMap;

/**
 * Enum representing the extended date time units supported for star tree index as part of index mapping.
 * The enum values are:
 * <ul>
 *     <li>HALF_HOUR_OF_DAY: Represents half hour of day rounding</li>
 *     <li>QUARTER_HOUR_OF_DAY: Represents quarter hour of day rounding</li>
 * </ul>
 * <p>
 * The enum also provides a static map of date field units to their corresponding ExtendedDateTimeUnit instances.
 *
 * @see org.opensearch.common.Rounding.DateTimeUnit for more information on the dateTimeUnit enum and rounding logic.
 *
 * @opensearch.experimental
 */
public enum DataCubeDateTimeUnit implements DateTimeUnitRounding {
    HALF_HOUR_OF_DAY("half-hour") {
        @Override
        public long roundFloor(long utcMillis) {
            return utcMillis - (utcMillis % TimeUnit.MINUTES.toMillis(30));
        }
    },
    QUARTER_HOUR_OF_DAY("quarter-hour") {
        @Override
        public long roundFloor(long utcMillis) {
            return utcMillis - (utcMillis % TimeUnit.MINUTES.toMillis(15));
        }
    };

    public static final Map<String, DataCubeDateTimeUnit> DATE_FIELD_UNITS;
    static {
        Map<String, DataCubeDateTimeUnit> dateFieldUnits = new HashMap<>();
        dateFieldUnits.put("30m", DataCubeDateTimeUnit.HALF_HOUR_OF_DAY);
        dateFieldUnits.put("half-hour", DataCubeDateTimeUnit.HALF_HOUR_OF_DAY);
        dateFieldUnits.put("15m", DataCubeDateTimeUnit.QUARTER_HOUR_OF_DAY);
        dateFieldUnits.put("quarter-hour", DataCubeDateTimeUnit.QUARTER_HOUR_OF_DAY);
        DATE_FIELD_UNITS = unmodifiableMap(dateFieldUnits);
    }

    private final String shortName;

    DataCubeDateTimeUnit(String shortName) {
        this.shortName = shortName;
    }

    /**
     * This rounds down the supplied milliseconds since the epoch down to the next unit. In order to retain performance this method
     * should be as fast as possible and not try to convert dates to java-time objects if possible
     *
     * @param utcMillis the milliseconds since the epoch
     * @return          the rounded down milliseconds since the epoch
     */
    @Override
    public abstract long roundFloor(long utcMillis);

    @Override
    public String shortName() {
        return shortName;
    }
}
