/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;

import static org.mockito.Mockito.mock;

public class TransportInterceptorRegistryTests extends OpenSearchTestCase {

    private TransportInterceptorRegistry transportInterceptorRegistry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        transportInterceptorRegistry = new TransportInterceptorRegistry();
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
    }

    public void testDefaultSettings() {
        assertEquals(transportInterceptorRegistry.transportInterceptors, new ArrayList<>());
        assertEquals(transportInterceptorRegistry.getTransportInterceptors().size(), 0);
    }

    public void testRegisterInterceptor() {
        transportInterceptorRegistry.addTransportInterceptor(mock(TransportInterceptor.class));
        assertEquals(transportInterceptorRegistry.getTransportInterceptors().size(), 1);
        transportInterceptorRegistry.addTransportInterceptor(mock(TransportInterceptor.class));
        assertEquals(transportInterceptorRegistry.getTransportInterceptors().size(), 2);
    }

    public void testGetAllInterceptor() {
        transportInterceptorRegistry.addTransportInterceptor(mock(TransportInterceptor.class));
        assertEquals(transportInterceptorRegistry.getTransportInterceptors().size(), 1);
        transportInterceptorRegistry.addTransportInterceptor(mock(TransportInterceptor.class));
        assertEquals(transportInterceptorRegistry.getTransportInterceptors().size(), 2);
        transportInterceptorRegistry.getTransportInterceptors().forEach(transportInterceptor -> {
            assertEquals(transportInterceptor.getClass(), mock(TransportInterceptor.class).getClass());
        });
    }
}
