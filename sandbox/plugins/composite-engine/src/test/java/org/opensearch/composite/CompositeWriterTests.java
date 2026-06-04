/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.index.engine.dataformat.FileInfos;
import org.opensearch.index.engine.dataformat.FlushInput;
import org.opensearch.index.engine.dataformat.WriterConfig;
import org.opensearch.index.engine.dataformat.WriterState;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

/**
 * Tests for {@link CompositeWriter}.
 */
public class CompositeWriterTests extends OpenSearchTestCase {

    private CompositeIndexingExecutionEngine engine;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        engine = CompositeTestHelper.createStubEngine("lucene", "parquet");
    }

    public void testWriterGenerationIsPreserved() throws IOException {
        long gen = randomLongBetween(0, 1000);
        CompositeWriter writer = new CompositeWriter(engine, new WriterConfig(gen));
        assertEquals(gen, writer.getWriterGeneration());
        writer.close();
    }

    public void testStateDefaultsToActive() throws IOException {
        CompositeWriter writer = new CompositeWriter(engine, new WriterConfig(0));
        assertEquals(WriterState.ACTIVE, writer.state());
        writer.close();
    }

    public void testStateAggregatesWorstChild() throws IOException {
        CompositeWriter writer = new CompositeWriter(engine, new WriterConfig(0));
        // No child has retired → composite is ACTIVE
        assertEquals(WriterState.ACTIVE, writer.state());

        // Force a child into RETIRED_FLUSHABLE — composite should reflect it (worst state wins)
        CompositeTestHelper.StubIndexingExecutionEngine primaryEngine = (CompositeTestHelper.StubIndexingExecutionEngine) engine
            .getPrimaryDelegate();
        primaryEngine.lastCreatedWriter.setState(WriterState.RETIRED_FLUSHABLE);
        assertEquals(WriterState.RETIRED_FLUSHABLE, writer.state());

        writer.close();
    }

    public void testCloseTransitionsToClosed() throws IOException {
        CompositeWriter writer = new CompositeWriter(engine, new WriterConfig(0));
        writer.close();
        assertEquals(WriterState.CLOSED, writer.state());
    }

    public void testFlushReturnsFileInfos() throws IOException {
        CompositeWriter writer = new CompositeWriter(engine, new WriterConfig(0));
        FileInfos fileInfos = writer.flush(FlushInput.EMPTY);
        assertNotNull(fileInfos);
        writer.close();
    }

    public void testSyncDoesNotThrow() throws IOException {
        CompositeWriter writer = new CompositeWriter(engine, new WriterConfig(0));
        writer.sync();
        writer.close();
    }

    public void testCloseDoesNotThrow() throws IOException {
        CompositeWriter writer = new CompositeWriter(engine, new WriterConfig(0));
        writer.close();
        // calling close again should also not throw
        writer.close();
    }

    public void testMappingVersionSetAtConstruction() throws IOException {
        CompositeWriter writer = new CompositeWriter(engine, new WriterConfig(0));
        assertEquals(0L, writer.mappingVersion());
        writer.close();
    }

    public void testUpdateMappingVersionPropagates() throws IOException {
        CompositeWriter writer = new CompositeWriter(engine, new WriterConfig(0));
        writer.updateMappingVersion(5L);
        assertEquals(5L, writer.mappingVersion());
        writer.close();
    }

    public void testIsSchemaMutableReturnsFalseWhenAnySubWriterImmutable() throws IOException {
        CompositeWriter writer = new CompositeWriter(engine, new WriterConfig(0));
        assertTrue(writer.isSchemaMutable());

        // Make the primary sub-writer immutable
        CompositeTestHelper.StubIndexingExecutionEngine primaryEngine = (CompositeTestHelper.StubIndexingExecutionEngine) engine
            .getPrimaryDelegate();
        primaryEngine.lastCreatedWriter.setSchemaMutable(false);
        assertFalse(writer.isSchemaMutable());

        writer.close();
    }
}
