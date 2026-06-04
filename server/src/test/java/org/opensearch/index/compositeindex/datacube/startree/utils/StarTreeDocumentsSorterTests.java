/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.utils;

import org.opensearch.common.Randomness;
import org.opensearch.common.Rounding;
import org.opensearch.index.compositeindex.datacube.DateDimension;
import org.opensearch.index.compositeindex.datacube.NumericDimension;
import org.opensearch.index.compositeindex.datacube.UnsignedLongDimension;
import org.opensearch.index.compositeindex.datacube.startree.utils.date.DateTimeUnitAdapter;
import org.opensearch.index.compositeindex.datacube.startree.utils.date.DateTimeUnitRounding;
import org.opensearch.index.mapper.DateFieldMapper;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Tests for {@link StarTreeDocumentsSorter}.
 */
public class StarTreeDocumentsSorterTests extends OpenSearchTestCase {

    private Map<Integer, Long[]> testData;
    private List<Comparator<Long>> comparatorList;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        testData = new HashMap<>();
        comparatorList = new ArrayList<>();

        List<DateTimeUnitRounding> intervals = Arrays.asList(
            new DateTimeUnitAdapter(Rounding.DateTimeUnit.YEAR_OF_CENTURY),
            new DateTimeUnitAdapter(Rounding.DateTimeUnit.MONTH_OF_YEAR),
            new DateTimeUnitAdapter(Rounding.DateTimeUnit.DAY_OF_MONTH),
            new DateTimeUnitAdapter(Rounding.DateTimeUnit.HOUR_OF_DAY)
        );
        DateDimension dateDimension = new DateDimension("timestamp", intervals, DateFieldMapper.Resolution.MILLISECONDS);
        Long[] date_dims = new Long[4];
        Long testValue = 1609459200000L; // 2021-01-01 00:00:00 UTC
        AtomicInteger dimIndex = new AtomicInteger(0);
        dateDimension.setDimensionValues(testValue, value -> { date_dims[dimIndex.getAndIncrement()] = value; });

        // 10 documents with 6 dimensions each
        testData.put(0, new Long[] { date_dims[0], date_dims[1], date_dims[2], date_dims[3], null, 150L, 100L, 300L, null });
        testData.put(1, new Long[] { date_dims[0], date_dims[1], date_dims[2], date_dims[3], 1L, null, -9223372036854775807L, 200L, 300L });
        testData.put(2, new Long[] { date_dims[0], date_dims[1], date_dims[2], date_dims[3], 2L, -100L, -15L, 250L, null });
        testData.put(
            3,
            new Long[] { date_dims[0], date_dims[1], date_dims[2], date_dims[3], 2L, -100L, -10L, 210L, -9223372036854775807L }
        );
        testData.put(4, new Long[] { date_dims[0], date_dims[1], date_dims[2], date_dims[3], 1L, 120L, null, null, 305L });
        testData.put(5, new Long[] { date_dims[0], date_dims[1], date_dims[2], date_dims[3], 2L, 150L, -5L, 200L, 295L });
        testData.put(6, new Long[] { date_dims[0], date_dims[1], date_dims[2], date_dims[3], 3L, 105L, null, -200L, -315L });
        testData.put(7, new Long[] { date_dims[0], date_dims[1], date_dims[2], date_dims[3], 1L, 120L, -10L, 205L, 310L });
        testData.put(
            8,
            new Long[] { date_dims[0], date_dims[1], date_dims[2], date_dims[3], null, -100L, 9223372036854775807L, 200L, -300L }
        );
        testData.put(9, new Long[] { date_dims[0], date_dims[1], date_dims[2], date_dims[3], 2L, null, -10L, 210L, 325L });

        comparatorList.addAll(Collections.nCopies(4, dateDimension.comparator()));
        comparatorList.add(new NumericDimension("dim1").comparator());
        comparatorList.add(new UnsignedLongDimension("dim2").comparator());
        comparatorList.add(new NumericDimension("dim3").comparator());
        comparatorList.add(new UnsignedLongDimension("dim4").comparator());
        comparatorList.add(new NumericDimension("dim5").comparator());

    }

    public void testSortDocumentsOffHeap_StartFromFirstDimension() {
        int[] sortedDocIds = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        int dimensionId = -1;
        int numDocs = 10;

        StarTreeDocumentsSorter.sort(sortedDocIds, dimensionId, numDocs, i -> testData.get(sortedDocIds[i]), comparatorList);
        assertArrayEquals(new int[] { 7, 4, 1, 5, 2, 3, 9, 6, 0, 8 }, sortedDocIds);
    }

    public void testSortDocumentsOffHeap_StartFromSecondDimension() {
        int[] sortedDocIds = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        int dimensionId = 0;
        int numDocs = 10;

        StarTreeDocumentsSorter.sort(sortedDocIds, dimensionId, numDocs, i -> testData.get(sortedDocIds[i]), comparatorList);
        assertArrayEquals(new int[] { 7, 4, 1, 5, 2, 3, 9, 6, 0, 8 }, sortedDocIds);
    }

    public void testSortDocumentsOffHeap_StartFromThirdDimension() {
        int[] sortedDocIds = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        int dimensionId = 1;
        int numDocs = 10;

        StarTreeDocumentsSorter.sort(sortedDocIds, dimensionId, numDocs, i -> testData.get(sortedDocIds[i]), comparatorList);
        assertArrayEquals(new int[] { 7, 4, 1, 5, 2, 3, 9, 6, 0, 8 }, sortedDocIds);
    }

    public void testSortDocumentsOffHeap_StartFromFourthDimension() {
        int[] sortedDocIds = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        int dimensionId = 2;
        int numDocs = 10;

        StarTreeDocumentsSorter.sort(sortedDocIds, dimensionId, numDocs, i -> testData.get(sortedDocIds[i]), comparatorList);
        assertArrayEquals(new int[] { 7, 4, 1, 5, 2, 3, 9, 6, 0, 8 }, sortedDocIds);
    }

    public void testSortDocumentsOffHeap_StartFromFifthDimension() {
        int[] sortedDocIds = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        int dimensionId = 3;
        int numDocs = 10;

        StarTreeDocumentsSorter.sort(sortedDocIds, dimensionId, numDocs, i -> testData.get(sortedDocIds[i]), comparatorList);
        assertArrayEquals(new int[] { 7, 4, 1, 5, 2, 3, 9, 6, 0, 8 }, sortedDocIds);
    }

    public void testSortDocumentsOffHeap_StartFromSixthDimension() {
        int[] sortedDocIds = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        int dimensionId = 4;
        int numDocs = 10;

        StarTreeDocumentsSorter.sort(sortedDocIds, dimensionId, numDocs, i -> testData.get(sortedDocIds[i]), comparatorList);
        assertArrayEquals(new int[] { 6, 7, 4, 5, 0, 2, 3, 8, 1, 9 }, sortedDocIds);
    }

    public void testSortDocumentsOffHeap_StartFromSeventhDimension() {
        int[] sortedDocIds = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        int dimensionId = 5;
        int numDocs = 10;

        StarTreeDocumentsSorter.sort(sortedDocIds, dimensionId, numDocs, i -> testData.get(sortedDocIds[i]), comparatorList);
        assertArrayEquals(new int[] { 1, 2, 7, 3, 9, 5, 0, 8, 6, 4 }, sortedDocIds);
    }

    public void testSortDocumentsOffHeap_SingleElement() {
        int[] sortedDocIds = { 0 };
        int dimensionId = -1;
        int numDocs = 1;

        StarTreeDocumentsSorter.sort(sortedDocIds, dimensionId, numDocs, i -> testData.get(sortedDocIds[i]), comparatorList);
        assertArrayEquals(new int[] { 0 }, sortedDocIds);
    }

    public void testSortDocumentsOffHeap_EmptyArray() {
        int[] sortedDocIds = {};
        int dimensionId = -1;
        int numDocs = 0;

        StarTreeDocumentsSorter.sort(sortedDocIds, dimensionId, numDocs, i -> testData.get(sortedDocIds[i]), comparatorList);
        assertArrayEquals(new int[] {}, sortedDocIds);
    }

    public void testSortDocumentsOffHeap_AllNulls() {
        Map<Integer, Long[]> testData = new HashMap<>();
        testData.put(0, new Long[] { null, null, null, null, null, null, null, null, null });
        testData.put(1, new Long[] { null, null, null, null, null, null, null, null, null });
        testData.put(2, new Long[] { null, null, null, null, null, null, null, null, null });

        int[] sortedDocIds = { 0, 1, 2 };
        int dimensionId = -1;
        int numDocs = 3;

        StarTreeDocumentsSorter.sort(sortedDocIds, dimensionId, numDocs, i -> testData.get(sortedDocIds[i]), comparatorList);

        // The order should remain unchanged as all elements are equal (null)
        assertArrayEquals(new int[] { 0, 1, 2 }, sortedDocIds);
    }

    public void testSortDocumentsOffHeap_Negatives() {
        Map<Integer, Long[]> testData = new HashMap<>();
        testData.put(0, new Long[] { -1L, -2L, -3L, -4L, -10L, 0L, null, 0L, -5L });
        testData.put(1, new Long[] { -5L, -2L, -3L, -4L, -9L, 0L, null, 0L, -10L });
        testData.put(2, new Long[] { -5L, -3L, -3L, -4L, -9L, 0L, null, 0L, 15L });
        testData.put(3, new Long[] { -9L, -2L, -3L, -4L, -7L, 0L, null, 0L, -20L });
        testData.put(4, new Long[] { -8L, -2L, -3L, -4L, -15L, 0L, null, 0L, -25L });

        int[] sortedDocIds = { 0, 1, 2, 3, 4 };
        int dimensionId = -1;
        int numDocs = 5;

        StarTreeDocumentsSorter.sort(sortedDocIds, dimensionId, numDocs, i -> testData.get(sortedDocIds[i]), comparatorList);
        assertArrayEquals(new int[] { 3, 4, 2, 1, 0 }, sortedDocIds);
    }

    public void testTheRandomSort() {
        int i = 0;
        while (i < 10) {
            testRandomizedSort();
            i++;
        }
    }

    private void testRandomizedSort() {

        int numDocs = randomIntBetween(0, 1000);
        Random random = Randomness.get();
        // skew more towards realistic number of dimensions
        int numDimensions = random.nextBoolean() ? randomIntBetween(2, 10) : randomIntBetween(2, 100);
        List<Long[]> testData = new ArrayList<>();
        // Generate random test data
        for (int i = 0; i < numDocs; i++) {
            Long[] dimensions = new Long[numDimensions];
            for (int j = 0; j < numDimensions; j++) {
                if (random.nextFloat() < 0.5) {
                    dimensions[j] = random.nextBoolean() ? Long.valueOf(0L) : random.nextBoolean() ? -1L : null;
                } else {
                    dimensions[j] = random.nextLong();
                }
            }
            testData.add(dimensions);
        }

        int[] sortedDocIds = new int[numDocs];
        for (int i = 0; i < numDocs; i++) {
            sortedDocIds[i] = i;
        }
        // sort dimensionId + 1 to numDimensions
        // for example to start from dimension in 0th index, we need to pass -1 to sort method
        int dimensionId = random.nextInt(numDimensions) - 1;
        List<Comparator<Long>> comparatorList = new ArrayList<>();

        for (int i = 0; i < numDimensions; i++) {
            Boolean isUnsignedLong = random.nextBoolean();
            if (!isUnsignedLong) comparatorList.add(new NumericDimension("fieldName").comparator());
            else comparatorList.add(new UnsignedLongDimension("fieldName").comparator());
        }

        // Sort using StarTreeDocumentsSorter
        StarTreeDocumentsSorter.sort(sortedDocIds, dimensionId, numDocs, i -> testData.get(sortedDocIds[i]), comparatorList);

        // Verify the sorting
        for (int i = 1; i < numDocs; i++) {
            Long[] prev = testData.get(sortedDocIds[i - 1]);
            Long[] curr = testData.get(sortedDocIds[i]);
            boolean isCorrectOrder = true;
            for (int j = dimensionId + 1; j < numDimensions; j++) {
                int comparison = comparatorList.get(j).compare(prev[j], curr[j]);
                if (comparison < 0) {
                    break;
                } else if (comparison > 0) {
                    isCorrectOrder = false;
                    break;
                }
            }
            assertTrue(
                "Sorting error when sorting from dimension index "
                    + dimensionId
                    + " Prev : "
                    + Arrays.toString(prev)
                    + " :: Curr : "
                    + Arrays.toString(curr),
                isCorrectOrder
            );
        }
    }

}
