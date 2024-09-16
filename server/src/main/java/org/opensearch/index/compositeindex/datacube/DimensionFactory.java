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
import org.opensearch.index.compositeindex.datacube.startree.StarTreeIndexSettings;
import org.opensearch.index.mapper.Mapper;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opensearch.index.compositeindex.datacube.DateDimension.CALENDAR_INTERVALS;

/**
 * Dimension factory class mainly used to parse and create dimension from the mappings
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DimensionFactory {
    public static Dimension parseAndCreateDimension(
        String name,
        String type,
        Map<String, Object> dimensionMap,
        Mapper.TypeParser.ParserContext c
    ) {
        switch (type) {
            case DateDimension.DATE:
                return parseAndCreateDateDimension(name, dimensionMap, c);
            case NumericDimension.NUMERIC:
                return new NumericDimension(name);
            default:
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "unsupported field type associated with dimension [%s] as part of star tree field", name)
                );
        }
    }

    public static Dimension parseAndCreateDimension(
        String name,
        Mapper.Builder builder,
        Map<String, Object> dimensionMap,
        Mapper.TypeParser.ParserContext c
    ) {
        if (builder.getSupportedDataCubeDimensionType().isPresent()
            && builder.getSupportedDataCubeDimensionType().get().equals(DimensionType.DATE)) {
            return parseAndCreateDateDimension(name, dimensionMap, c);
        } else if (builder.getSupportedDataCubeDimensionType().isPresent()
            && builder.getSupportedDataCubeDimensionType().get().equals(DimensionType.NUMERIC)) {
                return new NumericDimension(name);
            }
        throw new IllegalArgumentException(
            String.format(Locale.ROOT, "unsupported field type associated with star tree dimension [%s]", name)
        );
    }

    private static DateDimension parseAndCreateDateDimension(
        String name,
        Map<String, Object> dimensionMap,
        Mapper.TypeParser.ParserContext c
    ) {
        List<Rounding.DateTimeUnit> calendarIntervals = new ArrayList<>();
        List<String> intervalStrings = XContentMapValues.extractRawValues(CALENDAR_INTERVALS, dimensionMap)
            .stream()
            .map(Object::toString)
            .collect(Collectors.toList());
        if (intervalStrings == null || intervalStrings.isEmpty()) {
            calendarIntervals = StarTreeIndexSettings.DEFAULT_DATE_INTERVALS.get(c.getSettings());
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
            for (String interval : intervalStrings) {
                calendarIntervals.add(StarTreeIndexSettings.getTimeUnit(interval));
            }
            calendarIntervals = new ArrayList<>(calendarIntervals);
        }
        dimensionMap.remove(CALENDAR_INTERVALS);
        return new DateDimension(name, calendarIntervals);
    }
}
