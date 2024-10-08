/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube;

import org.opensearch.common.settings.Settings;
import org.opensearch.index.mapper.Mapper;
import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DimensionFactoryTests extends OpenSearchTestCase {

    public void testParseAndCreateDateDimension() {
        String name = "dateDimension";
        Map<String, Object> dimensionMap = new HashMap<>();
        List<String> calendarIntervals = Arrays.asList("day", "month");
        dimensionMap.put("calendar_intervals", calendarIntervals);

        Mapper.TypeParser.ParserContext mockContext = mock(Mapper.TypeParser.ParserContext.class);
        when(mockContext.getSettings()).thenReturn(Settings.EMPTY);

        Dimension dimension = DimensionFactory.parseAndCreateDimension(name, DateDimension.DATE, dimensionMap, mockContext);

        assertTrue(dimension instanceof DateDimension);
        assertEquals(2, dimension.getNumSubDimensions());
        for (String interval : calendarIntervals) {
            assertTrue(dimension.getSubDimensionNames().contains(name + "_" + interval));
        }
        assertEquals(name, dimension.getField());
        DateDimension dateDimension = (DateDimension) dimension;
        assertEquals(2, dateDimension.getIntervals().size());
    }

    public void testParseAndCreateNumericDimension() {
        String name = "numericDimension";
        Dimension dimension = DimensionFactory.parseAndCreateDimension(name, NumericDimension.NUMERIC, new HashMap<>(), null);

        assertTrue(dimension instanceof NumericDimension);
        assertEquals(1, dimension.getNumSubDimensions());
        assertTrue(dimension.getSubDimensionNames().contains(name));
        assertEquals(name, dimension.getField());
    }

    public void testParseAndCreateKeywordDimension() {
        String name = "keywordDimension";
        Dimension dimension = DimensionFactory.parseAndCreateDimension(name, KeywordDimension.KEYWORD, new HashMap<>(), null);
        KeywordDimension kd = new KeywordDimension(name);
        assertTrue(dimension instanceof KeywordDimension);
        assertEquals(1, dimension.getNumSubDimensions());
        assertTrue(dimension.getSubDimensionNames().contains(name));
        assertEquals(dimension, kd);
        assertEquals(name, dimension.getField());
        List<Long> dimValue = new ArrayList<>();
        kd.setDimensionValues(1L, dimValue::add);
        assertEquals((Long) 1L, dimValue.get(0));
    }

    public void testParseAndCreateDimensionWithUnsupportedType() {
        String name = "unsupportedDimension";
        String unsupportedType = "unsupported";

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> DimensionFactory.parseAndCreateDimension(name, unsupportedType, new HashMap<>(), null)
        );
        assertTrue(exception.getMessage().contains("unsupported field type"));
    }

    public void testParseAndCreateDimensionWithBuilder() {
        String name = "builderDimension";
        Mapper.Builder mockBuilder = mock(Mapper.Builder.class);
        when(mockBuilder.getSupportedDataCubeDimensionType()).thenReturn(java.util.Optional.of(DimensionType.KEYWORD));

        Dimension dimension = DimensionFactory.parseAndCreateDimension(name, mockBuilder, new HashMap<>(), null);

        assertTrue(dimension instanceof KeywordDimension);
        assertEquals(name, dimension.getField());
    }

    public void testParseAndCreateDimensionWithUnsupportedBuilder() {
        String name = "unsupportedBuilderDimension";
        Mapper.Builder mockBuilder = mock(Mapper.Builder.class);
        when(mockBuilder.getSupportedDataCubeDimensionType()).thenReturn(java.util.Optional.empty());

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> DimensionFactory.parseAndCreateDimension(name, mockBuilder, new HashMap<>(), null)
        );
        assertTrue(exception.getMessage().contains("unsupported field type"));
    }
}
