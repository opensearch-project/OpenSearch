/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex;

import org.opensearch.common.Rounding;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.compositeindex.startree.StarTreeIndexSettings;
import org.opensearch.index.mapper.Mapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
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

    public DateDimension(String name, List<Rounding.DateTimeUnit> intervals) {
        super(name);
        this.calendarIntervals = intervals;
    }

    @SuppressWarnings("unchecked")
    public DateDimension(Map.Entry<String, Object> dimension, Mapper.TypeParser.ParserContext c) {
        super(dimension.getKey());
        List<String> intervalStrings = XContentMapValues.extractRawValues("calendar_interval", (Map<String, Object>) dimension.getValue())
            .stream()
            .map(Object::toString)
            .collect(Collectors.toList());
        if (intervalStrings == null || intervalStrings.isEmpty()) {
            this.calendarIntervals = StarTreeIndexSettings.DEFAULT_DATE_INTERVALS.get(c.getSettings());
        } else {
            if (intervalStrings.size() > 3) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "At most 3 calendar intervals are allowed in dimension [%s]", dimension.getKey())
                );
            }
            Set<Rounding.DateTimeUnit> calendarIntervals = new HashSet<>();
            for (String interval : intervalStrings) {
                calendarIntervals.add(StarTreeIndexSettings.getTimeUnit(interval));
            }
            this.calendarIntervals = new ArrayList<>(calendarIntervals);
        }
    }

    public List<Rounding.DateTimeUnit> getIntervals() {
        return calendarIntervals;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(this.getField());
        builder.field("type", "date");
        builder.startArray("calendar_interval");
        for (Rounding.DateTimeUnit interval : calendarIntervals) {
            builder.value(interval.shortName());
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }
}
