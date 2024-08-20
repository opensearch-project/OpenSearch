/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.utils;

import org.opensearch.common.Randomness;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;

/**
 * Tests for {@link StarTreeDocumentsSorter}.
 */
public class StarTreeDocumentsSorterTests extends OpenSearchTestCase {
    private Map<Integer, Long[]> testData;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        testData = new HashMap<>();
        testData.put(0, new Long[] { -1L, 2L, 3L });
        testData.put(1, new Long[] { 1L, 2L, 2L });
        testData.put(2, new Long[] { -1L, -1L, 3L });
        testData.put(3, new Long[] { 1L, 2L, null });
        testData.put(4, new Long[] { 1L, null, 3L });
    }

    public void testSortDocumentsOffHeap_FirstDimension() {
        int[] sortedDocIds = { 0, 1, 2, 3, 4 };
        int dimensionId = -1;
        int numDocs = 5;

        StarTreeDocumentsSorter.sort(sortedDocIds, dimensionId, numDocs, i -> testData.get(sortedDocIds[i]));

        assertArrayEquals(new int[] { 2, 0, 1, 3, 4 }, sortedDocIds);
    }

    public void testSortDocumentsOffHeap_ThirdDimension() {
        int[] sortedDocIds = { 0, 1, 2, 3, 4 };
        int dimensionId = 1;
        int numDocs = 5;

        StarTreeDocumentsSorter.sort(sortedDocIds, dimensionId, numDocs, i -> testData.get(sortedDocIds[i]));

        assertArrayEquals(new int[] { 1, 0, 2, 4, 3 }, sortedDocIds);
    }

    public void testSortDocumentsOffHeap_SingleElement() {
        int[] sortedDocIds = { 0 };
        int dimensionId = -1;
        int numDocs = 1;

        StarTreeDocumentsSorter.sort(sortedDocIds, dimensionId, numDocs, i -> testData.get(sortedDocIds[i]));

        assertArrayEquals(new int[] { 0 }, sortedDocIds);
    }

    public void testSortDocumentsOffHeap_EmptyArray() {
        int[] sortedDocIds = {};
        int dimensionId = -1;
        int numDocs = 0;

        StarTreeDocumentsSorter.sort(sortedDocIds, dimensionId, numDocs, i -> testData.get(sortedDocIds[i]));

        assertArrayEquals(new int[] {}, sortedDocIds);
    }

    public void testSortDocumentsOffHeap_SecondDimensionId() {
        int[] sortedDocIds = { 0, 1, 2, 3, 4 };
        int dimensionId = 0;
        int numDocs = 5;

        StarTreeDocumentsSorter.sort(sortedDocIds, dimensionId, numDocs, i -> testData.get(sortedDocIds[i]));

        assertArrayEquals(new int[] { 2, 1, 0, 3, 4 }, sortedDocIds);
    }

    public void testSortDocumentsOffHeap_AllNulls() {
        Map<Integer, Long[]> testData = new HashMap<>();
        testData.put(0, new Long[] { null, null, null });
        testData.put(1, new Long[] { null, null, null });
        testData.put(2, new Long[] { null, null, null });

        int[] sortedDocIds = { 0, 1, 2 };
        int dimensionId = -1;
        int numDocs = 3;

        StarTreeDocumentsSorter.sort(sortedDocIds, dimensionId, numDocs, i -> testData.get(sortedDocIds[i]));

        // The order should remain unchanged as all elements are equal (null)
        assertArrayEquals(new int[] { 0, 1, 2 }, sortedDocIds);
    }

    public void testSortDocumentsOffHeap_Negatives() {
        Map<Integer, Long[]> testData = new HashMap<>();
        testData.put(0, new Long[] { -10L, 0L });
        testData.put(1, new Long[] { -9L, 0L });
        testData.put(2, new Long[] { -8L, 0L });
        testData.put(3, new Long[] { -7L, -0L });
        testData.put(4, new Long[] { -15L, -0L });

        int[] sortedDocIds = { 0, 1, 2, 3, 4 };
        int dimensionId = -1;
        int numDocs = 5;

        StarTreeDocumentsSorter.sort(sortedDocIds, dimensionId, numDocs, i -> testData.get(sortedDocIds[i]));

        // The order should remain unchanged as all elements are equal (null)
        assertArrayEquals(new int[] { 4, 0, 1, 2, 3 }, sortedDocIds);
    }

    public void testRandomSort() {
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

        // Sort using StarTreeDocumentsSorter
        StarTreeDocumentsSorter.sort(sortedDocIds, dimensionId, numDocs, i -> testData.get(sortedDocIds[i]));

        // Verify the sorting
        for (int i = 1; i < numDocs; i++) {
            Long[] prev = testData.get(sortedDocIds[i - 1]);
            Long[] curr = testData.get(sortedDocIds[i]);
            boolean isCorrectOrder = true;
            for (int j = dimensionId + 1; j < numDimensions; j++) {
                int comparison = compareLongs(prev[j], curr[j]);
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

    private int compareLongs(Long a, Long b) {
        if (!Objects.equals(a, b)) {
            if (a == null) {
                return 1;
            } else if (b == null) {
                return -1;
            } else {
                return a.compareTo(b);
            }
        }
        return 0;
    }
}
