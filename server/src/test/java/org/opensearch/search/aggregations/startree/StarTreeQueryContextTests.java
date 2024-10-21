/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.startree;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.FixedBitSet;
import org.opensearch.index.codec.composite.CompositeIndexFieldInfo;
import org.opensearch.search.startree.StarTreeQueryContext;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class StarTreeQueryContextTests extends OpenSearchTestCase {

    private CompositeIndexFieldInfo mockStarTree;
    private LeafReaderContext mockLeafReaderContext;
    private StarTreeQueryContext context;
    private Map<String, Long> queryMap;

    @Before
    public void setUp() {
        // Mock dependencies
        mockStarTree = mock(CompositeIndexFieldInfo.class);
        mockLeafReaderContext = mock(LeafReaderContext.class);

        // Prepare queryMap
        queryMap = new HashMap<>();
        queryMap.put("field1", 123L);
        queryMap.put("field2", 456L);

        // Simulate leaf reader context ord
        when(mockLeafReaderContext.ord).thenReturn(1);
    }

    public void testConstructorWithValidNumSegmentsCache() {
        // Initialize context with valid numSegmentsCache
        int numSegmentsCache = 3;
        context = new StarTreeQueryContext(mockStarTree, queryMap, numSegmentsCache);

        // Verify that the star tree and query map are set correctly
        assertEquals(mockStarTree, context.getStarTree());
        assertEquals(queryMap, context.getQueryMap());

        // Verify that the starTreeValues array is initialized correctly
        assertNotNull(context.getStarTreeValues());
        assertEquals(numSegmentsCache, context.getStarTreeValues().length);
    }

    public void testConstructorWithNegativeNumSegmentsCache() {
        // Initialize context with invalid numSegmentsCache (-1)
        context = new StarTreeQueryContext(mockStarTree, queryMap, -1);

        // Verify that the starTreeValues array is not initialized
        assertNull(context.getStarTreeValues());
    }

    public void testGetStarTreeValues_WithValidContext() {
        // Initialize context with a cache of size 3
        context = new StarTreeQueryContext(mockStarTree, queryMap, 3);

        // Create a FixedBitSet and assign it to the context
        FixedBitSet fixedBitSet = new FixedBitSet(10);
        context.setStarTreeValues(mockLeafReaderContext, fixedBitSet);

        // Retrieve the FixedBitSet for the given context
        FixedBitSet result = context.getStarTreeValues(mockLeafReaderContext);

        // Verify that the correct FixedBitSet is returned
        assertNotNull(result);
        assertEquals(fixedBitSet, result);
    }

    public void testGetStarTreeValues_WithNullCache() {
        // Initialize context with no cache (numSegmentsCache is -1)
        context = new StarTreeQueryContext(mockStarTree, queryMap, -1);

        // Retrieve the FixedBitSet for the given context
        FixedBitSet result = context.getStarTreeValues(mockLeafReaderContext);

        // Verify that the result is null since there's no cache
        assertNull(result);
    }

    public void testSetStarTreeValues() {
        // Initialize context with a cache of size 3
        context = new StarTreeQueryContext(mockStarTree, queryMap, 3);

        // Create a FixedBitSet and assign it to the context
        FixedBitSet fixedBitSet = new FixedBitSet(10);
        context.setStarTreeValues(mockLeafReaderContext, fixedBitSet);

        // Retrieve the FixedBitSet for the given context and verify the update
        FixedBitSet result = context.getStarTreeValues(mockLeafReaderContext);
        assertEquals(fixedBitSet, result);
    }

    public void testSetStarTreeValues_WithNullCache() {
        // Initialize context with no cache (numSegmentsCache is -1)
        context = new StarTreeQueryContext(mockStarTree, queryMap, -1);

        // Try setting a FixedBitSet and ensure no exception is thrown
        FixedBitSet fixedBitSet = new FixedBitSet(10);
        context.setStarTreeValues(mockLeafReaderContext, fixedBitSet);

        // Since the cache is null, getStarTreeValues should return null
        assertNull(context.getStarTreeValues(mockLeafReaderContext));
    }
}
