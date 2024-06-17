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
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeIndexSettings;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.index.mapper.StarTreeMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Date dimension class
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DateDimension extends Dimension {
    private final List<Rounding.DateTimeUnit> calendarIntervals;
    public static final String CALENDAR_INTERVALS = "calendar_intervals";
    public static final String DATE = "date";

    public DateDimension(String name, Map<String, Object> dimensionMap, Mapper.TypeParser.ParserContext c) {
        super(name);
        List<String> intervalStrings = XContentMapValues.extractRawValues(CALENDAR_INTERVALS, dimensionMap)
            .stream()
            .map(Object::toString)
            .collect(Collectors.toList());
        if (intervalStrings == null || intervalStrings.isEmpty()) {
            this.calendarIntervals = StarTreeIndexSettings.DEFAULT_DATE_INTERVALS.get(c.getSettings());
        } else {
            if (intervalStrings.size() > StarTreeIndexSettings.STAR_TREE_MAX_DATE_INTERVALS_SETTING.get(c.getSettings())) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "At most [%s] calendar intervals are allowed in dimension [%s]",
                        StarTreeIndexSettings.STAR_TREE_MAX_DATE_INTERVALS_SETTING.get(c.getSettings()),
                        name
                    )
                );
            }
            Set<Rounding.DateTimeUnit> calendarIntervals = new LinkedHashSet<>();
            for (String interval : intervalStrings) {
                calendarIntervals.add(StarTreeIndexSettings.getTimeUnit(interval));
            }
            this.calendarIntervals = new ArrayList<>(calendarIntervals);
        }
        dimensionMap.remove(CALENDAR_INTERVALS);
    }

    public List<Rounding.DateTimeUnit> getIntervals() {
        return calendarIntervals;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(StarTreeMapper.NAME, this.getField());
        builder.field(StarTreeMapper.TYPE, DATE);
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
        if (!super.equals(o)) return false;
        DateDimension that = (DateDimension) o;
        return Objects.equals(calendarIntervals, that.calendarIntervals);
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), calendarIntervals);
    }
}
