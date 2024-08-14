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
import org.opensearch.index.mapper.CompositeDataCubeFieldType;
import org.opensearch.index.mapper.DateFieldMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Date dimension class
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DateDimension implements Dimension {
    private final List<Rounding.DateTimeUnit> calendarIntervals;
    public static final String CALENDAR_INTERVALS = "calendar_intervals";
    public static final String DATE = "date";
    private final String field;
    private final List<Rounding.DateTimeUnit> sortedCalendarIntervals;
    private final DateFieldMapper.Resolution resolution;

    public DateDimension(String field, List<Rounding.DateTimeUnit> calendarIntervals, DateFieldMapper.Resolution resolution) {
        this.field = field;
        this.calendarIntervals = calendarIntervals;
        // Sort from the lowest unit to the highest unit
        this.sortedCalendarIntervals = Rounding.getSortedDateTimeUnits(calendarIntervals);
        if (resolution == null) {
            this.resolution = DateFieldMapper.Resolution.MILLISECONDS;
        } else {
            this.resolution = resolution;
        }
    }

    public List<Rounding.DateTimeUnit> getIntervals() {
        return calendarIntervals;
    }

    public List<Rounding.DateTimeUnit> getSortedCalendarIntervals() {
        return sortedCalendarIntervals;
    }

    /**
     * Sets the dimension values in sorted order in the provided array starting from the given index.
     *
     * @param val   The value to be set
     * @param dims  The dimensions array to set the values in
     * @param index The starting index in the array
     * @return The next available index in the array
     */
    @Override
    public int setDimensionValues(final Long val, final Long[] dims, int index) {
        for (Rounding.DateTimeUnit dateTimeUnit : sortedCalendarIntervals) {
            if (val == null) {
                dims[index++] = null;
                continue;
            }
            dims[index++] = dateTimeUnit.roundFloor(maybeConvertNanosToMillis(val));
        }
        return index;
    }

    /**
     * Converts nanoseconds to milliseconds based on the resolution of the field
     */
    private long maybeConvertNanosToMillis(long nanoSecondsSinceEpoch) {
        if (resolution.equals(DateFieldMapper.Resolution.NANOSECONDS)) return DateUtils.toMilliSeconds(nanoSecondsSinceEpoch);
        return nanoSecondsSinceEpoch;
    }

    /**
     * Returns the list of fields that represent the dimension
     */
    @Override
    public List<String> getDimensionFieldsNames() {
        List<String> fields = new ArrayList<>(calendarIntervals.size());
        for (Rounding.DateTimeUnit interval : sortedCalendarIntervals) {
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
        for (Rounding.DateTimeUnit interval : calendarIntervals) {
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
}
