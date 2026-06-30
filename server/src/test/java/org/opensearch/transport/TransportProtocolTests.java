/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport;

import org.opensearch.test.OpenSearchTestCase;

public class TransportProtocolTests extends OpenSearchTestCase {

    public void testNativeProtocol() {
        assertEquals(TransportProtocol.NATIVE, TransportProtocol.fromBytes((byte) 'E', (byte) 'S'));
    }

    public void testInvalidProtocol() {
        assertThrows(IllegalArgumentException.class, () -> TransportProtocol.fromBytes((byte) 'e', (byte) 'S'));
    }
}
