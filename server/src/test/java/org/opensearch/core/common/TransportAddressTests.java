/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.core.common;

import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.test.OpenSearchTestCase;

import java.net.UnknownHostException;

public class TransportAddressTests extends OpenSearchTestCase {
    public void testFromString() throws UnknownHostException {
        TransportAddress address = TransportAddress.fromString("127.0.0.1:9300");
        assertEquals("127.0.0.1", address.getAddress());
        assertEquals(9300, address.getPort());

        address = TransportAddress.fromString("1080:0:0:0:8:800:200C:417A:9300");
        assertEquals("1080::8:800:200c:417a", address.getAddress());
        assertEquals(9300, address.getPort());

        address = TransportAddress.fromString("FF01:0:0:0:0:0:0:101:9300");
        assertEquals("ff01::101", address.getAddress());
        assertEquals(9300, address.getPort());

        address = TransportAddress.fromString("FEDC:BA98:7654:3210:FEDC:BA98:7654:3210:9200");
        assertEquals("fedc:ba98:7654:3210:fedc:ba98:7654:3210", address.getAddress());
        assertEquals(9200, address.getPort());
    }
}
