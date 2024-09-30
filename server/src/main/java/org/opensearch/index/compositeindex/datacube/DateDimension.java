/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube;

import org.opensearch.common.Rounding;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.time.DateUtils;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.compositeindex.datacube.startree.utils.date.DataCubeDateTimeUnit;
import org.opensearch.index.compositeindex.datacube.startree.utils.date.DateTimeUnitRounding;
import org.opensearch.index.mapper.CompositeDataCubeFieldType;
import org.opensearch.index.mapper.DateFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * Date dimension class
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DateDimension implements Dimension {
    private final List<DateTimeUnitRounding> calendarIntervals;
    public static final String CALENDAR_INTERVALS = "calendar_intervals";
    public static final String DATE = "date";
    private final String field;
    private final List<DateTimeUnitRounding> sortedCalendarIntervals;
    private final DateFieldMapper.Resolution resolution;

    public DateDimension(String field, List<DateTimeUnitRounding> calendarIntervals, DateFieldMapper.Resolution resolution) {
        this.field = field;
        this.calendarIntervals = calendarIntervals;
        // Sort from the lowest unit to the highest unit
        this.sortedCalendarIntervals = getSortedDateTimeUnits(calendarIntervals);
        if (resolution == null) {
            this.resolution = DateFieldMapper.Resolution.MILLISECONDS;
        } else {
            this.resolution = resolution;
        }
    }

    public List<DateTimeUnitRounding> getIntervals() {
        return calendarIntervals;
    }

    public List<DateTimeUnitRounding> getSortedCalendarIntervals() {
        return sortedCalendarIntervals;
    }

    /**
     * Sets the dimension values in sorted order in the provided array starting from the given index.
     *
     * @param val   The value to be set
     * @param dimSetter  Consumer which sets the dimensions
     */
    @Override
    public void setDimensionValues(final Long val, final Consumer<Long> dimSetter) {
        for (DateTimeUnitRounding dateTimeUnit : sortedCalendarIntervals) {
            if (val == null) {
                dimSetter.accept(null);
            } else {
                Long roundedValue = dateTimeUnit.roundFloor(storedDurationSinceEpoch(val));
                dimSetter.accept(roundedValue);
            }
        }
    }

    /**
     * Converts nanoseconds to milliseconds based on the resolution of the field
     */
    private long storedDurationSinceEpoch(long nanoSecondsSinceEpoch) {
        if (resolution.equals(DateFieldMapper.Resolution.NANOSECONDS)) return DateUtils.toMilliSeconds(nanoSecondsSinceEpoch);
        return nanoSecondsSinceEpoch;
    }

    /**
     * Returns the list of fields that represent the dimension
     */
    @Override
    public List<String> getSubDimensionNames() {
        List<String> fields = new ArrayList<>(calendarIntervals.size());
        for (DateTimeUnitRounding interval : sortedCalendarIntervals) {
            // TODO : revisit this post file format changes
            fields.add(field + "_" + interval.shortName());
        }
        return fields;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("date_dimension");
        builder.field(CompositeDataCubeFieldType.NAME, this.getField());
        builder.field(CompositeDataCubeFieldType.TYPE, DATE);
        builder.startArray(CALENDAR_INTERVALS);
        for (DateTimeUnitRounding interval : calendarIntervals) {
            builder.value(interval.shortName());
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DateDimension that = (DateDimension) o;
        return Objects.equals(field, that.getField()) && Objects.equals(calendarIntervals, that.calendarIntervals);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, calendarIntervals);
    }

    @Override
    public String getField() {
        return field;
    }

    @Override
    public int getNumSubDimensions() {
        return calendarIntervals.size();
    }

    /**
     * DateTimeUnit Comparator which tracks dateTimeUnits in sorted order from second unit to year unit
     */
    public static class DateTimeUnitComparator implements Comparator<DateTimeUnitRounding> {
        public static final Map<String, Integer> ORDERED_DATE_TIME_UNIT = new HashMap<>();

        static {
            ORDERED_DATE_TIME_UNIT.put(Rounding.DateTimeUnit.SECOND_OF_MINUTE.shortName(), 1);
            ORDERED_DATE_TIME_UNIT.put(Rounding.DateTimeUnit.MINUTES_OF_HOUR.shortName(), 2);
            ORDERED_DATE_TIME_UNIT.put(DataCubeDateTimeUnit.QUARTER_HOUR_OF_DAY.shortName(), 3);
            ORDERED_DATE_TIME_UNIT.put(DataCubeDateTimeUnit.HALF_HOUR_OF_DAY.shortName(), 4);
            ORDERED_DATE_TIME_UNIT.put(Rounding.DateTimeUnit.HOUR_OF_DAY.shortName(), 5);
            ORDERED_DATE_TIME_UNIT.put(Rounding.DateTimeUnit.DAY_OF_MONTH.shortName(), 6);
            ORDERED_DATE_TIME_UNIT.put(Rounding.DateTimeUnit.WEEK_OF_WEEKYEAR.shortName(), 7);
            ORDERED_DATE_TIME_UNIT.put(Rounding.DateTimeUnit.MONTH_OF_YEAR.shortName(), 8);
            ORDERED_DATE_TIME_UNIT.put(Rounding.DateTimeUnit.QUARTER_OF_YEAR.shortName(), 9);
            ORDERED_DATE_TIME_UNIT.put(Rounding.DateTimeUnit.YEAR_OF_CENTURY.shortName(), 10);
        }

        @Override
        public int compare(DateTimeUnitRounding unit1, DateTimeUnitRounding unit2) {
            return Integer.compare(
                ORDERED_DATE_TIME_UNIT.getOrDefault(unit1.shortName(), Integer.MAX_VALUE),
                ORDERED_DATE_TIME_UNIT.getOrDefault(unit2.shortName(), Integer.MAX_VALUE)
            );
        }
    }

    /**
     * Returns a sorted list of dateTimeUnits based on the DateTimeUnitComparator
     */
    public static List<DateTimeUnitRounding> getSortedDateTimeUnits(List<DateTimeUnitRounding> dateTimeUnits) {
        return dateTimeUnits.stream().sorted(new DateTimeUnitComparator()).collect(Collectors.toList());
    }
}
