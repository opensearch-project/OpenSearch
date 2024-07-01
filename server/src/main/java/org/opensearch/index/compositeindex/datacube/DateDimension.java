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
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.mapper.CompositeDataCubeFieldType;

import java.io.IOException;
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

    public DateDimension(String field, List<Rounding.DateTimeUnit> calendarIntervals) {
        this.field = field;
        this.calendarIntervals = calendarIntervals;
    }

    public List<Rounding.DateTimeUnit> getIntervals() {
        return calendarIntervals;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
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
}
