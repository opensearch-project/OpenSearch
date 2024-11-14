/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.xcontent.support.XContentMapValues;
import org.opensearch.index.compositeindex.datacube.startree.StarTreeIndexSettings;
import org.opensearch.index.compositeindex.datacube.startree.utils.date.DateTimeUnitRounding;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.index.mapper.Mapper;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.opensearch.index.compositeindex.datacube.DateDimension.CALENDAR_INTERVALS;
import static org.opensearch.index.compositeindex.datacube.KeywordDimension.KEYWORD;

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
            case KEYWORD:
                return new KeywordDimension(name);
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
        if (builder.getSupportedDataCubeDimensionType().isEmpty()) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "unsupported field type associated with star tree dimension [%s]", name)
            );
        }
        switch (builder.getSupportedDataCubeDimensionType().get()) {
            case DATE:
                return parseAndCreateDateDimension(name, dimensionMap, c);
            case NUMERIC:
                return new NumericDimension(name);
            case KEYWORD:
                return new KeywordDimension(name);
            default:
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "unsupported field type associated with star tree dimension [%s]", name)
                );
        }
    }

    private static DateDimension parseAndCreateDateDimension(
        String name,
        Map<String, Object> dimensionMap,
        Mapper.TypeParser.ParserContext c
    ) {
        Set<DateTimeUnitRounding> calendarIntervals;
        List<String> intervalStrings = XContentMapValues.extractRawValues(CALENDAR_INTERVALS, dimensionMap)
            .stream()
            .map(Object::toString)
            .collect(Collectors.toList());
        if (intervalStrings == null || intervalStrings.isEmpty()) {
            calendarIntervals = new LinkedHashSet<>(StarTreeIndexSettings.DEFAULT_DATE_INTERVALS.get(c.getSettings()));
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
            calendarIntervals = new LinkedHashSet<>();
            for (String interval : intervalStrings) {
                calendarIntervals.add(StarTreeIndexSettings.getTimeUnit(interval));
            }
        }
        dimensionMap.remove(CALENDAR_INTERVALS);
        DateFieldMapper.Resolution resolution = null;
        if (c != null && c.mapperService() != null && c.mapperService().fieldType(name) != null) {
            resolution = ((DateFieldMapper.DateFieldType) c.mapperService().fieldType(name)).resolution();
        }

        return new DateDimension(name, new ArrayList<>(calendarIntervals), resolution);
    }
}
