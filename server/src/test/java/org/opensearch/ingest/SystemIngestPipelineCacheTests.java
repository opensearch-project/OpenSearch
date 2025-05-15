/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest;

import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import org.mockito.Mockito;

import static org.mockito.Mockito.mock;

public class SystemIngestPipelineCacheTests extends OpenSearchTestCase {
    private SystemIngestPipelineCache cache;
    private Pipeline dummyPipeline;

    @Before
    public void setup() {
        cache = new SystemIngestPipelineCache();
        dummyPipeline = new Pipeline("id", "description", null, new CompoundProcessor());
    }

    public void testCachePipelineAndRetrieve() {
        String index = "test_index";
        cache.cachePipeline(index, dummyPipeline, 10);

        Pipeline retrieved = cache.getSystemIngestPipeline(index);
        assertNotNull(retrieved);
        assertEquals(dummyPipeline, retrieved);
    }

    public void testCacheExceedMaxProcessorNumberThrowsException() {
        String index = "test_index";
        CompoundProcessor largeProcessor = mock(CompoundProcessor.class);
        Mockito.when(largeProcessor.getProcessors()).thenReturn(new java.util.ArrayList<>(11));
        Pipeline largePipeline = new Pipeline("id", "description", null, largeProcessor);

        try {
            cache.cachePipeline(index, largePipeline, 10);
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Too many system ingest processors"));
        }
    }

    public void testInvalidateCache() {
        String index = "test_index";
        cache.cachePipeline(index, dummyPipeline, 10);
        cache.invalidateCacheForIndex(index);

        assertNull(cache.getSystemIngestPipeline(index));
    }

    public void testEvictionWhenCacheIsFull() {
        for (int i = 0; i < 101; i++) {
            cache.cachePipeline("index_" + i, dummyPipeline, 10);
        }

        // The cache should not exceed 100 entries
        assertEquals(100, cache.size());
    }
}
