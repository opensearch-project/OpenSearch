/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.compositeindex.datacube.startree.utils;

import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

/**
 * Tests for {@link StarTreeDocumentsSorter}.
 */
public class StarTreeDocumentsSorterTests extends OpenSearchTestCase {
    private StarTreeDocumentsSorter sorter;
    private Map<Integer, Long[]> testData;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        sorter = new StarTreeDocumentsSorter();
        testData = new HashMap<>();
        testData.put(0, new Long[] { 1L, 2L, 3L });
        testData.put(1, new Long[] { 1L, 2L, 2L });
        testData.put(2, new Long[] { 1L, 1L, 3L });
        testData.put(3, new Long[] { 1L, 2L, null });
        testData.put(4, new Long[] { 1L, null, 3L });
    }

    public void testSortDocumentsOffHeap_FirstDimension() {
        int[] sortedDocIds = { 0, 1, 2, 3, 4 };
        int dimensionId = -1;
        int numDocs = 5;

        sorter.sort(sortedDocIds, dimensionId, numDocs, i -> testData.get(i));

        assertArrayEquals(new int[] { 2, 1, 0, 3, 4 }, sortedDocIds);
    }

    public void testSortDocumentsOffHeap_ThirdDimension() {
        int[] sortedDocIds = { 0, 1, 2, 3, 4 };
        int dimensionId = 1;
        int numDocs = 5;

        sorter.sort(sortedDocIds, dimensionId, numDocs, i -> testData.get(i));

        assertArrayEquals(new int[] { 1, 0, 2, 4, 3 }, sortedDocIds);
    }

    public void testSortDocumentsOffHeap_SingleElement() {
        int[] sortedDocIds = { 0 };
        int dimensionId = -1;
        int numDocs = 1;

        sorter.sort(sortedDocIds, dimensionId, numDocs, i -> testData.get(i));

        assertArrayEquals(new int[] { 0 }, sortedDocIds);
    }

    public void testSortDocumentsOffHeap_EmptyArray() {
        int[] sortedDocIds = {};
        int dimensionId = -1;
        int numDocs = 0;

        sorter.sort(sortedDocIds, dimensionId, numDocs, i -> testData.get(i));

        assertArrayEquals(new int[] {}, sortedDocIds);
    }

    public void testSortDocumentsOffHeap_SecondDimensionId() {
        int[] sortedDocIds = { 0, 1, 2, 3, 4 };
        int dimensionId = 0;
        int numDocs = 5;

        sorter.sort(sortedDocIds, dimensionId, numDocs, i -> testData.get(i));

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

        sorter.sort(sortedDocIds, dimensionId, numDocs, i -> testData.get(i));

        // The order should remain unchanged as all elements are equal (null)
        assertArrayEquals(new int[] { 0, 1, 2 }, sortedDocIds);
    }
}
