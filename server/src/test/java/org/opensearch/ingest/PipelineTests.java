/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class PipelineTests extends OpenSearchTestCase {

    public void testCreatePipelineWithEmptyConfig() {
        final Pipeline pipeline = Pipeline.createSystemIngestPipeline("test-index", Collections.emptyMap(), Collections.emptyMap());

        assertNotNull(pipeline);
        assertEquals("test-index_index_based_ingest_pipeline", pipeline.getId());
        assertTrue(pipeline.getProcessors().isEmpty());
    }

    public void testCreatePipelineWithOneProcessor() throws Exception {
        final Processor processor = mock(Processor.class);
        final Processor.Factory factory = mock(Processor.Factory.class);
        final Map<String, Object> config = Map.of("key", "value");
        when(factory.create(any(), any(), any(), any())).thenReturn(processor);

        final Pipeline pipeline = Pipeline.createSystemIngestPipeline("my-index", Map.of("factory", factory), config);

        assertNotNull(pipeline);
        assertEquals("my-index_index_based_ingest_pipeline", pipeline.getId());
        assertEquals(1, pipeline.getProcessors().size());
        assertSame(processor, pipeline.getProcessors().get(0));

        verify(factory, times(1)).create(any(), any(), any(), any());
    }

    public void testCreatePipelineWithFactoryException() throws Exception {
        final Map<String, Object> config = Map.of("key", "value");
        final Processor.Factory faultyFactory = mock(Processor.Factory.class);
        when(faultyFactory.create(any(), any(), any(), any())).thenThrow(new RuntimeException("Factory failed"));

        final RuntimeException e = assertThrows(
            RuntimeException.class,
            () -> Pipeline.createSystemIngestPipeline("my-index", Map.of("factory", faultyFactory), config)
        );
        assertTrue(e.getMessage().contains("Factory failed"));
    }
}
