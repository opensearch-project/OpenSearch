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
import org.opensearch.index.compositeindex.datacube.startree.utils.date.DateTimeUnitAdapter;
import org.opensearch.index.compositeindex.datacube.startree.utils.date.DateTimeUnitRounding;
import org.opensearch.index.compositeindex.datacube.startree.utils.date.ExtendedDateTimeUnit;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.opensearch.index.compositeindex.datacube.DateDimension.DateTimeUnitComparator.ORDERED_DATE_TIME_UNIT;

public class DateDimensionTests extends OpenSearchTestCase {
    public void testDateDimension() {
        String field = "timestamp";
        List<DateTimeUnitRounding> intervals = Arrays.asList(
            ExtendedDateTimeUnit.HALF_HOUR_OF_DAY,
            new DateTimeUnitAdapter(Rounding.DateTimeUnit.MONTH_OF_YEAR),
            new DateTimeUnitAdapter(Rounding.DateTimeUnit.YEAR_OF_CENTURY)
        );
        DateDimension dateDimension = new DateDimension(field, intervals, DateFieldMapper.Resolution.MILLISECONDS);

        assertEquals(field, dateDimension.getField());
        assertEquals(intervals, dateDimension.getIntervals());
        for (int i = 0; i < intervals.size(); i++) {
            assertEquals(intervals.get(i).shortName(), dateDimension.getSortedCalendarIntervals().get(i).shortName());
        }
    }

    public void testSetDimensionValuesWithMultipleIntervalsYear() {
        List<DateTimeUnitRounding> intervals = Arrays.asList(
            new DateTimeUnitAdapter(Rounding.DateTimeUnit.YEAR_OF_CENTURY),
            new DateTimeUnitAdapter(Rounding.DateTimeUnit.MONTH_OF_YEAR),
            new DateTimeUnitAdapter(Rounding.DateTimeUnit.DAY_OF_MONTH),
            new DateTimeUnitAdapter(Rounding.DateTimeUnit.HOUR_OF_DAY)
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
        List<DateTimeUnitRounding> allUnits = getAllTimeUnits();

        DateDimension dateDimension = new DateDimension("timestamp", allUnits, DateFieldMapper.Resolution.NANOSECONDS);

        // Test sorting
        List<DateTimeUnitRounding> sortedUnits = dateDimension.getSortedCalendarIntervals();
        assertEquals(allUnits.size(), sortedUnits.size());
        for (int i = 0; i < sortedUnits.size() - 1; i++) {
            assertTrue(
                "Units should be sorted in ascending order",
                ORDERED_DATE_TIME_UNIT.get(sortedUnits.get(i).shortName())
                    .compareTo(ORDERED_DATE_TIME_UNIT.get(sortedUnits.get(i + 1).shortName())) <= 0
            );
        }

        // Test rounding
        long testValueNanos = 1655293505382719622L; // 2022-06-15T11:45:05.382719622Z
        Long[] dims = new Long[allUnits.size()];
        dateDimension.setDimensionValues(testValueNanos, dims, 0);

        // Expected rounded values (in nanoseconds)
        long secondRounded = 1655293505000L; // 2022-06-15T11:45:05Z
        long minuteRounded = 1655293500000L; // 2022-06-15T11:45:00Z
        long quarterHourRounded = 1655293500000L; // 2022-06-15T11:45:00Z
        long halfHourRounded = 1655292600000L; // 2022-06-15T11:30:00Z
        long hourRounded = 1655290800000L; // 2022-06-15T11:00:00Z
        long dayRounded = 1655251200000L; // 2022-06-15T00:00:00Z
        long weekRounded = 1655078400000L; // 2022-06-13T00:00:00Z (Monday)
        long monthRounded = 1654041600000L; // 2022-06-01T00:00:00Z
        long quarterRounded = 1648771200000L; // 2022-04-01T00:00:00Z
        long yearRounded = 1640995200000L; // 2022-01-01T00:00:00Z

        assertTimeUnits(
            sortedUnits,
            dims,
            secondRounded,
            minuteRounded,
            hourRounded,
            dayRounded,
            weekRounded,
            monthRounded,
            quarterRounded,
            yearRounded,
            halfHourRounded,
            quarterHourRounded
        );
    }

    public void testRoundingAndSortingAllDateTimeUnitsMillis() {
        List<DateTimeUnitRounding> allUnits = getAllTimeUnits();

        DateDimension dateDimension = new DateDimension("timestamp", allUnits, DateFieldMapper.Resolution.MILLISECONDS);

        // Test sorting
        List<DateTimeUnitRounding> sortedUnits = dateDimension.getSortedCalendarIntervals();
        assertEquals(allUnits.size(), sortedUnits.size());
        for (int i = 0; i < sortedUnits.size() - 1; i++) {
            assertTrue(
                "Units should be sorted in ascending order",
                ORDERED_DATE_TIME_UNIT.get(sortedUnits.get(i).shortName())
                    .compareTo(ORDERED_DATE_TIME_UNIT.get(sortedUnits.get(i + 1).shortName())) <= 0
            );
        }

        // Test rounding
        long testValueNanos = 1724114825234L; // 2024-08-20T00:47:05.234Z
        Long[] dims = new Long[allUnits.size()];
        dateDimension.setDimensionValues(testValueNanos, dims, 0);

        // Expected rounded values (in millis)
        long secondRounded = 1724114825000L; // 2024-08-20T00:47:05.000Z
        long minuteRounded = 1724114820000L; // 2024-08-20T00:47:00.000Z
        long quarterHourRounded = 1724114700000L; // 2024-08-20T00:45:00.000Z
        long halfHourRounded = 1724113800000L; // 2024-08-20T00:30:00.000Z
        long hourRounded = 1724112000000L; // 2024-08-20T00:00:00.000Z
        long dayRounded = 1724112000000L; // 2024-08-20T00:00:00.000Z
        long weekRounded = 1724025600000L; // 2024-08-15T00:00:00.000Z (Monday)
        long monthRounded = 1722470400000L; // 2024-08-01T00:00:00.000Z
        long quarterRounded = 1719792000000L; // 2024-07-01T00:00:00.000Z
        long yearRounded = 1704067200000L; // 2024-01-01T00:00:00.000Z

        assertTimeUnits(
            sortedUnits,
            dims,
            secondRounded,
            minuteRounded,
            hourRounded,
            dayRounded,
            weekRounded,
            monthRounded,
            quarterRounded,
            yearRounded,
            halfHourRounded,
            quarterHourRounded
        );
    }

    private static List<DateTimeUnitRounding> getAllTimeUnits() {
        return Arrays.asList(
            new DateTimeUnitAdapter(Rounding.DateTimeUnit.WEEK_OF_WEEKYEAR),
            new DateTimeUnitAdapter(Rounding.DateTimeUnit.MONTH_OF_YEAR),
            new DateTimeUnitAdapter(Rounding.DateTimeUnit.QUARTER_OF_YEAR),
            new DateTimeUnitAdapter(Rounding.DateTimeUnit.YEAR_OF_CENTURY),
            new DateTimeUnitAdapter(Rounding.DateTimeUnit.SECOND_OF_MINUTE),
            new DateTimeUnitAdapter(Rounding.DateTimeUnit.MINUTES_OF_HOUR),
            new DateTimeUnitAdapter(Rounding.DateTimeUnit.HOUR_OF_DAY),
            new DateTimeUnitAdapter(Rounding.DateTimeUnit.DAY_OF_MONTH),
            ExtendedDateTimeUnit.HALF_HOUR_OF_DAY,
            ExtendedDateTimeUnit.QUARTER_HOUR_OF_DAY
        );
    }

    private static void assertTimeUnits(
        List<DateTimeUnitRounding> sortedUnits,
        Long[] dims,
        long secondRounded,
        long minuteRounded,
        long hourRounded,
        long dayRounded,
        long weekRounded,
        long monthRounded,
        long quarterRounded,
        long yearRounded,
        long halfHourRounded,
        long quarterHourRounded
    ) {
        for (int i = 0; i < sortedUnits.size(); i++) {
            DateTimeUnitRounding unit = sortedUnits.get(i);
            String unitName = unit.shortName();
            switch (unitName) {
                case "second":
                    assertEquals(secondRounded, (long) dims[i]);
                    break;
                case "minute":
                    assertEquals(minuteRounded, (long) dims[i]);
                    break;
                case "hour":
                    assertEquals(hourRounded, (long) dims[i]);
                    break;
                case "day":
                    assertEquals(dayRounded, (long) dims[i]);
                    break;
                case "week":
                    assertEquals(weekRounded, (long) dims[i]);
                    break;
                case "month":
                    assertEquals(monthRounded, (long) dims[i]);
                    break;
                case "quarter":
                    assertEquals(quarterRounded, (long) dims[i]);
                    break;
                case "year":
                    assertEquals(yearRounded, (long) dims[i]);
                    break;
                case "half-hour":
                    assertEquals(halfHourRounded, (long) dims[i]);
                    break;
                case "quarter-hour":
                    assertEquals(quarterHourRounded, (long) dims[i]);
                    break;
                default:
                    fail("Unexpected DateTimeUnit: " + unit);
            }
        }
    }

    public void testGetDimensionFieldsNames() {
        DateDimension dateDimension = new DateDimension(
            "timestamp",
            Arrays.asList(
                new DateTimeUnitAdapter(Rounding.DateTimeUnit.HOUR_OF_DAY),
                new DateTimeUnitAdapter(Rounding.DateTimeUnit.DAY_OF_MONTH)
            ),
            DateFieldMapper.Resolution.MILLISECONDS
        );

        List<String> fields = dateDimension.getDimensionFieldsNames();

        assertEquals(2, fields.size());
        assertEquals("timestamp_hour", fields.get(0));
        assertEquals("timestamp_day", fields.get(1));
    }

    public void testGetExtendedTimeUnitFieldsNames() {
        DateDimension dateDimension = new DateDimension(
            "timestamp",
            Arrays.asList(ExtendedDateTimeUnit.HALF_HOUR_OF_DAY, ExtendedDateTimeUnit.QUARTER_HOUR_OF_DAY),
            DateFieldMapper.Resolution.MILLISECONDS
        );

        List<String> fields = dateDimension.getDimensionFieldsNames();

        assertEquals(2, fields.size());
        assertEquals("timestamp_quarter-hour", fields.get(0));
        assertEquals("timestamp_half-hour", fields.get(1));
    }

    public void testSetDimensionValues() {
        DateDimension dateDimension = new DateDimension(
            "timestamp",
            Arrays.asList(
                new DateTimeUnitAdapter(Rounding.DateTimeUnit.YEAR_OF_CENTURY),
                new DateTimeUnitAdapter(Rounding.DateTimeUnit.HOUR_OF_DAY)
            ),
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
        Comparator<DateTimeUnitRounding> comparator = new DateDimension.DateTimeUnitComparator();
        assertTrue(
            comparator.compare(
                new DateTimeUnitAdapter(Rounding.DateTimeUnit.SECOND_OF_MINUTE),
                new DateTimeUnitAdapter(Rounding.DateTimeUnit.MINUTES_OF_HOUR)
            ) < 0
        );
        assertTrue(
            comparator.compare(
                new DateTimeUnitAdapter(Rounding.DateTimeUnit.HOUR_OF_DAY),
                new DateTimeUnitAdapter(Rounding.DateTimeUnit.DAY_OF_MONTH)
            ) < 0
        );
        assertTrue(
            comparator.compare(
                new DateTimeUnitAdapter(Rounding.DateTimeUnit.YEAR_OF_CENTURY),
                new DateTimeUnitAdapter(Rounding.DateTimeUnit.MONTH_OF_YEAR)
            ) > 0
        );
        assertEquals(
            0,
            comparator.compare(
                new DateTimeUnitAdapter(Rounding.DateTimeUnit.WEEK_OF_WEEKYEAR),
                new DateTimeUnitAdapter(Rounding.DateTimeUnit.WEEK_OF_WEEKYEAR)
            )
        );
    }
}
