/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.distributed;

import org.opensearch.index.shard.IndexShard;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;


public class IndexShardContextTests extends OpenSearchTestCase {

    private IndexShard mockIndexShard;
    private IndexShardContext context;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        mockIndexShard = mock(IndexShard.class);
    }

    public void testGetPrimaryTermWithValidIndexShard() {
        long expectedPrimaryTerm = 5L;
        when(mockIndexShard.getOperationPrimaryTerm()).thenReturn(expectedPrimaryTerm);
        
        context = new IndexShardContext(mockIndexShard);
        
        assertEquals(expectedPrimaryTerm, context.getPrimaryTerm());
        assertTrue(context.isAvailable());
    }

    public void testGetPrimaryTermWithNullIndexShard() {
        context = new IndexShardContext(null);
        
        assertEquals(IndexShardContext.DEFAULT_PRIMARY_TERM, context.getPrimaryTerm());
        assertFalse(context.isAvailable());
    }

    public void testGetPrimaryTermWithExceptionFromIndexShard() {
        when(mockIndexShard.getOperationPrimaryTerm()).thenThrow(new RuntimeException("Test exception"));
        
        context = new IndexShardContext(mockIndexShard);
        
        assertEquals(IndexShardContext.DEFAULT_PRIMARY_TERM, context.getPrimaryTerm());
        assertTrue(context.isAvailable()); // IndexShard is available, but throws exception
    }

    public void testPrimaryTermCaching() {
        long primaryTerm1 = 3L;
        long primaryTerm2 = 4L;
        
        when(mockIndexShard.getOperationPrimaryTerm()).thenReturn(primaryTerm1);
        
        context = new IndexShardContext(mockIndexShard);
        
        // First call should fetch from IndexShard
        assertEquals(primaryTerm1, context.getPrimaryTerm());
        
        // Second call within cache duration should return cached value
        assertEquals(primaryTerm1, context.getPrimaryTerm());
        
        // Change the mock to return different value
        when(mockIndexShard.getOperationPrimaryTerm()).thenReturn(primaryTerm2);
        
        // Should still return cached value
        assertEquals(primaryTerm1, context.getPrimaryTerm());
        
        // Invalidate cache and verify new value is fetched
        context.invalidateCache();
        assertEquals(primaryTerm2, context.getPrimaryTerm());
    }

    public void testCacheExpiration() throws InterruptedException {
        long primaryTerm1 = 7L;
        long primaryTerm2 = 8L;
        
        when(mockIndexShard.getOperationPrimaryTerm()).thenReturn(primaryTerm1);
        
        context = new IndexShardContext(mockIndexShard);
        
        // First call
        assertEquals(primaryTerm1, context.getPrimaryTerm());
        assertTrue(context.isCacheValid());
        
        // Change mock return value
        when(mockIndexShard.getOperationPrimaryTerm()).thenReturn(primaryTerm2);
        
        // Wait for cache to expire (cache duration is 1 second)
        Thread.sleep(1100);
        
        // Should fetch new value after cache expiration
        assertEquals(primaryTerm2, context.getPrimaryTerm());
    }

    public void testInvalidateCache() {
        long primaryTerm = 10L;
        when(mockIndexShard.getOperationPrimaryTerm()).thenReturn(primaryTerm);
        
        context = new IndexShardContext(mockIndexShard);
        
        // Populate cache
        assertEquals(primaryTerm, context.getPrimaryTerm());
        assertTrue(context.isCacheValid());
        
        // Invalidate cache
        context.invalidateCache();
        assertFalse(context.isCacheValid());
        assertEquals(-1L, context.getCachedPrimaryTerm());
    }

    public void testGetIndexShard() {
        context = new IndexShardContext(mockIndexShard);
        assertEquals(mockIndexShard, context.getIndexShard());
        
        context = new IndexShardContext(null);
        assertNull(context.getIndexShard());
    }
}