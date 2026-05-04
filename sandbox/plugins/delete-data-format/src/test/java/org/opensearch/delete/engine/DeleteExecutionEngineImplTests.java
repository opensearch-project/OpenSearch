/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.delete.engine;

import org.opensearch.index.engine.dataformat.Deleter;
import org.opensearch.index.engine.dataformat.DocumentInput;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.dataformat.stub.MockDataFormat;
import org.opensearch.index.engine.dataformat.stub.MockIndexingExecutionEngine;
import org.opensearch.test.OpenSearchTestCase;

@SuppressWarnings("unchecked")
public class DeleteExecutionEngineImplTests extends OpenSearchTestCase {

    private MockDataFormat dataFormat;
    private DeleteExecutionEngineImpl engine;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        dataFormat = new MockDataFormat();
        engine = new DeleteExecutionEngineImpl(dataFormat);
    }

    public void testGetDataFormat() {
        assertEquals(dataFormat, engine.getDataFormat());
    }

    public void testCreateDeleterAndGetDeleter() {
        MockIndexingExecutionEngine indexEngine = new MockIndexingExecutionEngine(dataFormat);
        Writer<DocumentInput<?>> writer = (Writer<DocumentInput<?>>) (Writer<?>) indexEngine.createWriter(1L);

        Deleter deleter = engine.createDeleter(writer);
        assertNotNull(deleter);

        Deleter retrieved = engine.getDeleter(writer);
        assertSame(deleter, retrieved);
    }

    public void testGetDeleterReturnsNullForUnknownWriter() {
        MockIndexingExecutionEngine indexEngine = new MockIndexingExecutionEngine(dataFormat);
        Writer<DocumentInput<?>> writer = (Writer<DocumentInput<?>>) (Writer<?>) indexEngine.createWriter(1L);

        assertNull(engine.getDeleter(writer));
    }

    public void testMultipleWritersGetDistinctDeleters() {
        MockIndexingExecutionEngine indexEngine = new MockIndexingExecutionEngine(dataFormat);
        Writer<DocumentInput<?>> writer1 = (Writer<DocumentInput<?>>) (Writer<?>) indexEngine.createWriter(1L);
        Writer<DocumentInput<?>> writer2 = (Writer<DocumentInput<?>>) (Writer<?>) indexEngine.createWriter(2L);

        Deleter deleter1 = engine.createDeleter(writer1);
        Deleter deleter2 = engine.createDeleter(writer2);

        assertNotSame(deleter1, deleter2);
        assertSame(deleter1, engine.getDeleter(writer1));
        assertSame(deleter2, engine.getDeleter(writer2));
    }
}
