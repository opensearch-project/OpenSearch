/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight.impl;

import org.apache.arrow.flight.FlightStream;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.arrow.flight.bootstrap.ServerConfig;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FlightStreamReaderTests extends OpenSearchTestCase {

    private FlightStream mockFlightStream;

    private FlightStreamReader iterator;
    private VectorSchemaRoot root;
    private BufferAllocator allocator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        ServerConfig.init(Settings.EMPTY);
        mockFlightStream = mock(FlightStream.class);
        allocator = new RootAllocator(100000);
        Field field = new Field("id", FieldType.nullable(new ArrowType.Int(32, true)), null);
        Schema schema = new Schema(List.of(field));
        root = VectorSchemaRoot.create(schema, allocator);
        when(mockFlightStream.getRoot()).thenReturn(root);
        iterator = new FlightStreamReader(mockFlightStream);
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        root.close();
        allocator.close();
    }

    public void testNext_ReturnsTrue_WhenFlightStreamHasNext() throws Exception {
        when(mockFlightStream.next()).thenReturn(true);
        assertTrue(iterator.next());
        assert (mockFlightStream).next();
    }

    public void testNext_ReturnsFalse_WhenFlightStreamHasNoNext() throws Exception {
        when(mockFlightStream.next()).thenReturn(false);
        assertFalse(iterator.next());
        verify(mockFlightStream).next();
    }

    public void testGetRoot_ReturnsRootFromFlightStream() throws Exception {
        VectorSchemaRoot returnedRoot = iterator.getRoot();
        assertEquals(root, returnedRoot);
        verify(mockFlightStream).getRoot();
    }

    public void testClose_CallsCloseOnFlightStream() throws Exception {
        iterator.close();
        verify(mockFlightStream).close();
    }

    public void testClose_WrapsExceptionInRuntimeException() throws Exception {
        doThrow(new Exception("Test exception")).when(mockFlightStream).close();
        assertThrows(RuntimeException.class, () -> iterator.close());
        verify(mockFlightStream).close();
    }
}
