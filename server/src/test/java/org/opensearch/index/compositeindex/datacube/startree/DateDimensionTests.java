/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree;

import org.opensearch.common.Rounding;
import org.opensearch.index.compositeindex.datacube.DateDimension;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.opensearch.common.Rounding.DateTimeUnitComparator.ORDERED_DATE_TIME_UNIT;

public class DateDimensionTests extends OpenSearchTestCase {
    public void testDateDimension() {
        String field = "timestamp";
        List<Rounding.DateTimeUnit> intervals = Arrays.asList(
            Rounding.DateTimeUnit.YEAR_OF_CENTURY,
            Rounding.DateTimeUnit.HOUR_OF_DAY,
            Rounding.DateTimeUnit.MONTH_OF_YEAR
        );
        DateDimension dateDimension = new DateDimension(field, intervals, DateFieldMapper.Resolution.MILLISECONDS);

        assertEquals(field, dateDimension.getField());
        assertEquals(intervals, dateDimension.getIntervals());
        assertEquals(
            Arrays.asList(Rounding.DateTimeUnit.HOUR_OF_DAY, Rounding.DateTimeUnit.MONTH_OF_YEAR, Rounding.DateTimeUnit.YEAR_OF_CENTURY),
            dateDimension.getSortedCalendarIntervals()
        );
    }

    public void testSetDimensionValuesWithMultipleIntervalsYear() {
        List<Rounding.DateTimeUnit> intervals = Arrays.asList(
            Rounding.DateTimeUnit.YEAR_OF_CENTURY,
            Rounding.DateTimeUnit.MONTH_OF_YEAR,
            Rounding.DateTimeUnit.DAY_OF_MONTH,
            Rounding.DateTimeUnit.HOUR_OF_DAY
        );
        DateDimension dateDimension = new DateDimension("timestamp", intervals, DateFieldMapper.Resolution.MILLISECONDS);
        Long[] dims = new Long[4];
        Long testValue = 1609459200000L; // 2021-01-01 00:00:00 UTC

        int nextIndex = dateDimension.setDimensionValues(testValue, dims, 0);

        assertEquals(4, nextIndex);
        assertEquals(1609459200000L, (long) dims[0]); // Hour rounded
        assertEquals(1609459200000L, (long) dims[1]); // Day rounded
        assertEquals(1609459200000L, (long) dims[2]); // Month rounded
        assertEquals(1609459200000L, (long) dims[3]); // Year rounded
        assertEquals(4, dateDimension.getNumSubDimensions());
    }

    public void testRoundingAndSortingAllDateTimeUnitsNanos() {
        List<Rounding.DateTimeUnit> allUnits = Arrays.asList(
            Rounding.DateTimeUnit.WEEK_OF_WEEKYEAR,
            Rounding.DateTimeUnit.MONTH_OF_YEAR,
            Rounding.DateTimeUnit.QUARTER_OF_YEAR,
            Rounding.DateTimeUnit.YEAR_OF_CENTURY,
            Rounding.DateTimeUnit.SECOND_OF_MINUTE,
            Rounding.DateTimeUnit.MINUTES_OF_HOUR,
            Rounding.DateTimeUnit.HOUR_OF_DAY,
            Rounding.DateTimeUnit.DAY_OF_MONTH
        );

        DateDimension dateDimension = new DateDimension("timestamp", allUnits, DateFieldMapper.Resolution.NANOSECONDS);

        // Test sorting
        List<Rounding.DateTimeUnit> sortedUnits = dateDimension.getSortedCalendarIntervals();
        assertEquals(allUnits.size(), sortedUnits.size());
        for (int i = 0; i < sortedUnits.size() - 1; i++) {
            assertTrue(
                "Units should be sorted in ascending order",
                ORDERED_DATE_TIME_UNIT.get(sortedUnits.get(i)).compareTo(ORDERED_DATE_TIME_UNIT.get(sortedUnits.get(i + 1))) <= 0
            );
        }

        // Test rounding
        long testValueNanos = 1655287972382719622L; // 2022-06-15T10:12:52.382719622Z
        Long[] dims = new Long[allUnits.size()];
        dateDimension.setDimensionValues(testValueNanos, dims, 0);

        // Expected rounded values (in nanoseconds)
        long secondRounded = 1655287972000L; // 2022-06-15T10:12:52
        long minuteRounded = 1655287920000L; // 2022-06-15T10:12:00
        long hourRounded = 1655287200000L; // 2022-06-15T10:00:00
        long dayRounded = 1655251200000L; // 2022-06-15T00:00:00
        long weekRounded = 1655078400000L; // 2022-06-13T00:00:00 // Monday
        long monthRounded = 1654041600000L; // 2022-06-01T00:00:00
        long quarterRounded = 1648771200000L; // 2022-04-01T00:00:00
        long yearRounded = 1640995200000L; // 2022-01-01T00:00:00

        for (int i = 0; i < sortedUnits.size(); i++) {
            Rounding.DateTimeUnit unit = sortedUnits.get(i);
            switch (unit) {
                case SECOND_OF_MINUTE:
                    assertEquals(secondRounded, (long) dims[i]);
                    break;
                case MINUTES_OF_HOUR:
                    assertEquals(minuteRounded, (long) dims[i]);
                    break;
                case HOUR_OF_DAY:
                    assertEquals(hourRounded, (long) dims[i]);
                    break;
                case DAY_OF_MONTH:
                    assertEquals(dayRounded, (long) dims[i]);
                    break;
                case WEEK_OF_WEEKYEAR:
                    assertEquals(weekRounded, (long) dims[i]);
                    break;
                case MONTH_OF_YEAR:
                    assertEquals(monthRounded, (long) dims[i]);
                    break;
                case QUARTER_OF_YEAR:
                    assertEquals(quarterRounded, (long) dims[i]);
                    break;
                case YEAR_OF_CENTURY:
                    assertEquals(yearRounded, (long) dims[i]);
                    break;
                default:
                    fail("Unexpected DateTimeUnit: " + unit);
            }
        }
    }

    public void testRoundingAndSortingAllDateTimeUnitsMillis() {
        List<Rounding.DateTimeUnit> allUnits = Arrays.asList(
            Rounding.DateTimeUnit.WEEK_OF_WEEKYEAR,
            Rounding.DateTimeUnit.MONTH_OF_YEAR,
            Rounding.DateTimeUnit.QUARTER_OF_YEAR,
            Rounding.DateTimeUnit.YEAR_OF_CENTURY,
            Rounding.DateTimeUnit.SECOND_OF_MINUTE,
            Rounding.DateTimeUnit.MINUTES_OF_HOUR,
            Rounding.DateTimeUnit.HOUR_OF_DAY,
            Rounding.DateTimeUnit.DAY_OF_MONTH
        );

        DateDimension dateDimension = new DateDimension("timestamp", allUnits, DateFieldMapper.Resolution.MILLISECONDS);

        // Test sorting
        List<Rounding.DateTimeUnit> sortedUnits = dateDimension.getSortedCalendarIntervals();
        assertEquals(allUnits.size(), sortedUnits.size());
        for (int i = 0; i < sortedUnits.size() - 1; i++) {
            assertTrue(
                "Units should be sorted in ascending order",
                ORDERED_DATE_TIME_UNIT.get(sortedUnits.get(i)).compareTo(ORDERED_DATE_TIME_UNIT.get(sortedUnits.get(i + 1))) <= 0
            );
        }

        // Test rounding
        long testValueNanos = 1655287972382L; // 2022-06-15T10:12:52.382Z

        Long[] dims = new Long[allUnits.size()];
        dateDimension.setDimensionValues(testValueNanos, dims, 0);

        // Expected rounded values (in millis)
        long secondRounded = 1655287972000L; // 2022-06-15T10:12:52
        long minuteRounded = 1655287920000L; // 2022-06-15T10:12:00
        long hourRounded = 1655287200000L; // 2022-06-15T10:00:00
        long dayRounded = 1655251200000L; // 2022-06-15T00:00:00
        long weekRounded = 1655078400000L; // 2022-06-13T00:00:00 // Monday
        long monthRounded = 1654041600000L; // 2022-06-01T00:00:00
        long quarterRounded = 1648771200000L; // 2022-04-01T00:00:00
        long yearRounded = 1640995200000L; // 2022-01-01T00:00:00

        for (int i = 0; i < sortedUnits.size(); i++) {
            Rounding.DateTimeUnit unit = sortedUnits.get(i);
            switch (unit) {
                case SECOND_OF_MINUTE:
                    assertEquals(secondRounded, (long) dims[i]);
                    break;
                case MINUTES_OF_HOUR:
                    assertEquals(minuteRounded, (long) dims[i]);
                    break;
                case HOUR_OF_DAY:
                    assertEquals(hourRounded, (long) dims[i]);
                    break;
                case DAY_OF_MONTH:
                    assertEquals(dayRounded, (long) dims[i]);
                    break;
                case WEEK_OF_WEEKYEAR:
                    assertEquals(weekRounded, (long) dims[i]);
                    break;
                case MONTH_OF_YEAR:
                    assertEquals(monthRounded, (long) dims[i]);
                    break;
                case QUARTER_OF_YEAR:
                    assertEquals(quarterRounded, (long) dims[i]);
                    break;
                case YEAR_OF_CENTURY:
                    assertEquals(yearRounded, (long) dims[i]);
                    break;
                default:
                    fail("Unexpected DateTimeUnit: " + unit);
            }
        }
    }

    public void testGetDimensionFieldsNames() {
        DateDimension dateDimension = new DateDimension(
            "timestamp",
            Arrays.asList(Rounding.DateTimeUnit.HOUR_OF_DAY, Rounding.DateTimeUnit.DAY_OF_MONTH),
            DateFieldMapper.Resolution.MILLISECONDS
        );

        List<String> fields = dateDimension.getDimensionFieldsNames();

        assertEquals(2, fields.size());
        assertEquals("timestamp_hour", fields.get(0));
        assertEquals("timestamp_day", fields.get(1));
    }

    public void testSetDimensionValues() {
        DateDimension dateDimension = new DateDimension(
            "timestamp",
            Arrays.asList(Rounding.DateTimeUnit.YEAR_OF_CENTURY, Rounding.DateTimeUnit.HOUR_OF_DAY),
            DateFieldMapper.Resolution.MILLISECONDS
        );
        Long[] dims = new Long[2];
        Long testValue = 1609459200000L; // 2021-01-01 00:00:00 UTC

        int nextIndex = dateDimension.setDimensionValues(testValue, dims, 0);

        assertEquals(2, nextIndex);
        assertEquals(1609459200000L, (long) dims[0]); // Hour rounded
        assertEquals(1609459200000L, (long) dims[1]); // Year rounded
    }

    public void testDateTimeUnitComparator() {
        Comparator<Rounding.DateTimeUnit> comparator = new Rounding.DateTimeUnitComparator();
        assertTrue(comparator.compare(Rounding.DateTimeUnit.SECOND_OF_MINUTE, Rounding.DateTimeUnit.MINUTES_OF_HOUR) < 0);
        assertTrue(comparator.compare(Rounding.DateTimeUnit.HOUR_OF_DAY, Rounding.DateTimeUnit.DAY_OF_MONTH) < 0);
        assertTrue(comparator.compare(Rounding.DateTimeUnit.YEAR_OF_CENTURY, Rounding.DateTimeUnit.MONTH_OF_YEAR) > 0);
        assertEquals(0, comparator.compare(Rounding.DateTimeUnit.WEEK_OF_WEEKYEAR, Rounding.DateTimeUnit.WEEK_OF_WEEKYEAR));
    }
}
