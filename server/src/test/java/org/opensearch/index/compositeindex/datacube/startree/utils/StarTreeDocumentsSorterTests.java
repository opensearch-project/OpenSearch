/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.utils;

import org.opensearch.common.Randomness;
import org.opensearch.index.compositeindex.datacube.Dimension;
import org.opensearch.index.compositeindex.datacube.NumericDimension;
import org.opensearch.index.compositeindex.datacube.UnsignedLongDimension;
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
    private List<Dimension> dimensionsOrder;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        testData = new HashMap<>();

        // 10 documents with 5 dimensions each
        testData.put(0, new Long[] { null, 150L, 100L, 300L, null });
        testData.put(1, new Long[] { 1L, null, -9223372036854775807L, 200L, 300L });
        testData.put(2, new Long[] { 2L, -100L, -15L, 250L, null });
        testData.put(3, new Long[] { 2L, -100L, -10L, 210L, -9223372036854775807L });
        testData.put(4, new Long[] { 1L, 120L, null, null, 305L });
        testData.put(5, new Long[] { 2L, 150L, -5L, 200L, 295L });
        testData.put(6, new Long[] { 3L, 105L, null, -200L, -315L });
        testData.put(7, new Long[] { 1L, 120L, -10L, 205L, 310L });
        testData.put(8, new Long[] { null, -100L, 9223372036854775807L, 200L, -300L });
        testData.put(9, new Long[] { 2L, null, -10L, 210L, 325L });

        dimensionsOrder = Arrays.asList(
            new NumericDimension("dim1"),  // Long
            new UnsignedLongDimension("dim2"),   // Unsigned Long
            new NumericDimension("dim3"),  // Long
            new UnsignedLongDimension("dim4"),   // Unsigned Long
            new NumericDimension("dim5")   // Long
        );
    }

    public void testSortDocumentsOffHeap_FirstDimension() {
        int[] sortedDocIds = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        int dimensionId = -1;
        int numDocs = 10;

        StarTreeDocumentsSorter.sort(sortedDocIds, dimensionId, numDocs, i -> testData.get(sortedDocIds[i]), dimensionsOrder);
        assertArrayEquals(new int[] { 7, 4, 1, 5, 2, 3, 9, 6, 0, 8 }, sortedDocIds);
    }

    public void testSortDocumentsOffHeap_SecondDimension() {
        int[] sortedDocIds = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        int dimensionId = 0;
        int numDocs = 10;

        StarTreeDocumentsSorter.sort(sortedDocIds, dimensionId, numDocs, i -> testData.get(sortedDocIds[i]), dimensionsOrder);
        assertArrayEquals(new int[] { 6, 7, 4, 5, 0, 2, 3, 8, 1, 9 }, sortedDocIds);
    }

    public void testSortDocumentsOffHeap_ThirdDimension() {
        int[] sortedDocIds = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        int dimensionId = 1;
        int numDocs = 10;

        StarTreeDocumentsSorter.sort(sortedDocIds, dimensionId, numDocs, i -> testData.get(sortedDocIds[i]), dimensionsOrder);
        assertArrayEquals(new int[] { 1, 2, 7, 3, 9, 5, 0, 8, 6, 4 }, sortedDocIds);
    }

    public void testSortDocumentsOffHeap_FourthDimension() {
        int[] sortedDocIds = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        int dimensionId = 2;
        int numDocs = 10;

        StarTreeDocumentsSorter.sort(sortedDocIds, dimensionId, numDocs, i -> testData.get(sortedDocIds[i]), dimensionsOrder);
        assertArrayEquals(new int[] { 8, 5, 1, 7, 3, 9, 2, 0, 6, 4 }, sortedDocIds);
    }

    public void testSortDocumentsOffHeap_FifthDimension() {
        int[] sortedDocIds = { 0, 1, 2, 3, 4, 5, 6, 7, 8, 9 };
        int dimensionId = 3;
        int numDocs = 10;

        StarTreeDocumentsSorter.sort(sortedDocIds, dimensionId, numDocs, i -> testData.get(sortedDocIds[i]), dimensionsOrder);
        assertArrayEquals(new int[] { 3, 6, 8, 5, 1, 4, 7, 9, 0, 2 }, sortedDocIds);
    }

    public void testSortDocumentsOffHeap_SingleElement() {
        int[] sortedDocIds = { 0 };
        int dimensionId = -1;
        int numDocs = 1;

        StarTreeDocumentsSorter.sort(sortedDocIds, dimensionId, numDocs, i -> testData.get(sortedDocIds[i]), dimensionsOrder);
        assertArrayEquals(new int[] { 0 }, sortedDocIds);
    }

    public void testSortDocumentsOffHeap_EmptyArray() {
        int[] sortedDocIds = {};
        int dimensionId = -1;
        int numDocs = 0;

        StarTreeDocumentsSorter.sort(sortedDocIds, dimensionId, numDocs, i -> testData.get(sortedDocIds[i]), dimensionsOrder);
        assertArrayEquals(new int[] {}, sortedDocIds);
    }

    public void testSortDocumentsOffHeap_AllNulls() {
        Map<Integer, Long[]> testData = new HashMap<>();
        testData.put(0, new Long[] { null, null, null, null, null });
        testData.put(1, new Long[] { null, null, null, null, null });
        testData.put(2, new Long[] { null, null, null, null, null });

        int[] sortedDocIds = { 0, 1, 2 };
        int dimensionId = -1;
        int numDocs = 3;

        StarTreeDocumentsSorter.sort(sortedDocIds, dimensionId, numDocs, i -> testData.get(sortedDocIds[i]), dimensionsOrder);

        // The order should remain unchanged as all elements are equal (null)
        assertArrayEquals(new int[] { 0, 1, 2 }, sortedDocIds);
    }

    public void testSortDocumentsOffHeap_Negatives() {
        Map<Integer, Long[]> testData = new HashMap<>();
        testData.put(0, new Long[] { -10L, 0L, null, 0L, -5L });
        testData.put(1, new Long[] { -9L, 0L, null, 0L, -10L });
        testData.put(2, new Long[] { -9L, 0L, null, 0L, 15L });
        testData.put(3, new Long[] { -7L, 0L, null, 0L, -20L });
        testData.put(4, new Long[] { -15L, 0L, null, 0L, -25L });

        int[] sortedDocIds = { 0, 1, 2, 3, 4 };
        int dimensionId = -1;
        int numDocs = 5;

        StarTreeDocumentsSorter.sort(sortedDocIds, dimensionId, numDocs, i -> testData.get(sortedDocIds[i]), dimensionsOrder);
        assertArrayEquals(new int[] { 4, 0, 1, 2, 3 }, sortedDocIds);
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

        List<Dimension> dimensionsOrder = new ArrayList<>();
        for (int i = 0; i < numDimensions; i++) {
            Boolean isUnsignedLong = random.nextBoolean();

            if (isUnsignedLong) dimensionsOrder.add(new NumericDimension("fieldName"));
            else dimensionsOrder.add(new UnsignedLongDimension("fieldName"));
        }

        // Sort using StarTreeDocumentsSorter
        StarTreeDocumentsSorter.sort(sortedDocIds, dimensionId, numDocs, i -> testData.get(sortedDocIds[i]), dimensionsOrder);

        // Verify the sorting
        for (int i = 1; i < numDocs; i++) {
            Long[] prev = testData.get(sortedDocIds[i - 1]);
            Long[] curr = testData.get(sortedDocIds[i]);
            boolean isCorrectOrder = true;
            for (int j = dimensionId + 1; j < numDimensions; j++) {
                int comparison = -1;
                if (dimensionsOrder.get(j) instanceof UnsignedLongDimension) comparison = compareLongs(prev[j], curr[j], true);
                else comparison = compareLongs(prev[j], curr[j], false);
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

    private int compareLongs(Long a, Long b, Boolean isUnsignedLong) {
        if (!Objects.equals(a, b)) {
            if (a == null) {
                return 1;
            } else if (b == null) {
                return -1;
            } else {
                return isUnsignedLong ? Long.compareUnsigned(a, b) : Long.compare(a, b);
            }
        }
        return 0;
    }
}
